package main

import (
	"flag"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

var (
	fetchConfigPath     = flag.String("fetch-config", "", "Path to YAML/JSON run config used for fetch testing")
	fetchRemoteOverride = flag.String("fetch-remote-path", "", "Override remote artifact directory (optional)")
	fetchDestOverride   = flag.String("fetch-dest", "", "Override local destination directory (optional)")
	fetchPatternFlag    multiStringFlag
	fetchSinceOverride  = flag.String("fetch-since-start", "", "Override since-start filter (true/false). Leave empty to use config/experiment default.")
	fetchDryRun         = flag.Bool("fetch-dry-run", false, "List files without copying them")

	gitRemoteHost = flag.String("git-remote-host", "", "Remote host (user@host) for git info testing")
	gitRemoteDir  = flag.String("git-remote-dir", "", "Remote directory containing a git repo for git info testing")
)

func init() {
	flag.Var(&fetchPatternFlag, "fetch-pattern", "Regex applied to remote paths during test; may repeat")
}

func TestFetchFromConfig(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping fetch test in short mode")
	}
	if *fetchConfigPath == "" {
		t.Skip("set -fetch-config to run this integration test")
	}

	absConfig, err := filepath.Abs(*fetchConfigPath)
	if err != nil {
		t.Fatalf("config path: %v", err)
	}
	cfg, err := loadRunConfigFile(absConfig)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if cfg == nil {
		t.Fatalf("config file %s is empty", absConfig)
	}

	remotePath := firstNonEmpty(*fetchRemoteOverride, cfg.ArtifactRemote)
	if remotePath == "" {
		t.Fatal("artifact remote path is required (use -fetch-remote-path or set artifact_remote in config)")
	}
	if !strings.HasPrefix(remotePath, "/") {
		t.Fatalf("remote path must be absolute, got %s", remotePath)
	}

	destDir := firstNonEmpty(*fetchDestOverride, cfg.ArtifactDest)
	if destDir == "" {
		t.Fatal("artifact destination is required (use -fetch-dest or set artifact_dest in config)")
	}

	patterns := fetchPatternFlag.Values()
	if len(patterns) == 0 {
		patterns = normalizePatternList(cfg.ArtifactPattern, cfg.ArtifactPatterns)
	}
	sinceStart := false
	if cfg.ArtifactSinceStart != nil {
		sinceStart = *cfg.ArtifactSinceStart
	}
	if *fetchSinceOverride != "" {
		val, err := strconv.ParseBool(*fetchSinceOverride)
		if err != nil {
			t.Fatalf("parse fetch-since-start: %v", err)
		}
		sinceStart = val
	}

	remoteHost := firstNonEmpty(cfg.Remote, os.Getenv("EXP_REMOTE"))
	if remoteHost == "" {
		t.Fatal("remote host is required (set remote in config or EXP_REMOTE)")
	}

	exp := &Experiment{
		Name:               cfg.Name,
		Remote:             remoteHost,
		ArtifactRemote:     remotePath,
		ArtifactDest:       destDir,
		ArtifactPattern:    combinePatterns(patterns),
		ArtifactSinceStart: sinceStart,
		CreatedAt:          time.Now().UTC(),
	}

	if err := fetchArtifacts(exp, remotePath, destDir, patterns, sinceStart, *fetchDryRun); err != nil {
		t.Fatalf("fetchArtifacts: %v", err)
	}
	files, _, err := listRemoteFiles(remoteHost, remotePath, time.Time{})
	if err == nil {
		if len(files) == 0 {
			t.Logf("No files reported under %s during logging pass", remotePath)
		} else {
			t.Logf("Files visible under %s:\n%s", remotePath, strings.Join(files, "\n"))
		}
	} else {
		t.Logf("Unable to list files for logging: %v", err)
	}
	if *fetchDryRun {
		t.Logf("Dry-run complete for %s -> %s (patterns: %v)", remotePath, destDir, patterns)
	} else {
		t.Logf("Artifacts copied to %s (patterns: %v)", destDir, patterns)
	}
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func TestRemoteGitInfo(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping git test in short mode")
	}
	if *gitRemoteHost == "" || *gitRemoteDir == "" {
		t.Skip("set -git-remote-host and -git-remote-dir to run git info test")
	}
	if !strings.HasPrefix(*gitRemoteDir, "/") {
		t.Fatalf("git remote dir must be absolute, got %s", *gitRemoteDir)
	}
	commit, branch, err := getRemoteGitInfo(*gitRemoteHost, *gitRemoteDir)
	if err != nil {
		t.Fatalf("getRemoteGitInfo: %v", err)
	}
	if commit == "" || branch == "" {
		t.Fatalf("remote git info empty (commit=%q, branch=%q)", commit, branch)
	}
	t.Logf("remote git info from %s:%s -> commit=%s branch=%s", *gitRemoteHost, *gitRemoteDir, commit, branch)
}
