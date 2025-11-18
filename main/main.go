package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite" // SQLite driver (pure Go)
)

type Experiment struct {
	ID          int64
	Name        string
	Remote      string
	ScriptPath  string
	Args        string
	GitCommit   string
	GitBranch   string
	JobID       string
	JobStatus   string
	LogPath     string
	CreatedAt   time.Time
	CompletedAt time.Time

	ArtifactRemote     string
	ArtifactDest       string
	ArtifactSources    []ArtifactSource
	ArtifactPattern    string
	ArtifactSinceStart bool
	ArtifactLastSync   time.Time
	ArtifactLastError  string

	ConfigSnapshot string
}

const (
	defaultPollInterval   = 30 * time.Second
	sinceStartGracePeriod = 2 * time.Minute
	artifactSettleDelay   = 10 * time.Second
)

type boolFlag struct {
	value bool
	set   bool
}

func (b *boolFlag) Set(s string) error {
	val, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	b.value = val
	b.set = true
	return nil
}

func (b *boolFlag) String() string {
	return strconv.FormatBool(b.value)
}

func (b *boolFlag) IsBoolFlag() bool {
	return true
}

type multiStringFlag struct {
	values []string
}

func (m *multiStringFlag) Set(s string) error {
	m.values = append(m.values, s)
	return nil
}

func (m *multiStringFlag) String() string {
	return strings.Join(m.values, ",")
}

func (m *multiStringFlag) Values() []string {
	return append([]string(nil), m.values...)
}

type durationFlag struct {
	value time.Duration
	set   bool
}

func (d *durationFlag) Set(s string) error {
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	d.value = v
	d.set = true
	return nil
}

func (d *durationFlag) String() string {
	return d.value.String()
}

type Config struct {
	Defaults RunProfile            `json:"defaults"`
	Profiles map[string]RunProfile `json:"profiles"`
	path     string                `json:"-"`
}

type RunProfile struct {
	Remote             string           `json:"remote"`
	LogDir             string           `json:"log_dir"`
	Script             string           `json:"script"`
	BuildScript        string           `json:"build_script"`
	ArtifactSources    []ArtifactSource `json:"artifact_sources"`
	ArtifactPatterns   []string         `json:"artifact_patterns"`
	ArtifactRemote     string           `json:"artifact_remote"`
	ArtifactDest       string           `json:"artifact_dest"`
	ArtifactPattern    string           `json:"artifact_pattern"`
	ArtifactSinceStart *bool            `json:"artifact_since_start"`
	PollInterval       string           `json:"poll_interval"`
}

type RunConfigFile struct {
	Profile            string           `json:"profile"`
	Name               string           `json:"name"`
	Remote             string           `json:"remote"`
	LogDir             string           `json:"log_dir"`
	Script             string           `json:"script"`
	BuildScript        string           `json:"build_script"`
	ScriptLocal        string           `json:"script_local"`
	ArtifactRemote     string           `json:"artifact_remote"`
	ArtifactDest       string           `json:"artifact_dest"`
	ArtifactSources    []ArtifactSource `json:"artifact_sources"`
	ArtifactPatterns   []string         `json:"artifact_patterns"`
	ArtifactPattern    string           `json:"artifact_pattern"`
	ArtifactSinceStart *bool            `json:"artifact_since_start"`
	PollInterval       string           `json:"poll_interval"`
	Args               []string         `json:"args"`
}

type RunSnapshot struct {
	Name               string           `json:"name"`
	Remote             string           `json:"remote"`
	LogDir             string           `json:"log_dir"`
	Script             string           `json:"script"`
	BuildScript        string           `json:"build_script,omitempty"`
	ArtifactRemote     string           `json:"artifact_remote"`
	ArtifactDest       string           `json:"artifact_dest"`
	ArtifactPatterns   []string         `json:"artifact_patterns,omitempty"`
	ArtifactSources    []ArtifactSource `json:"artifact_sources,omitempty"`
	ArtifactPattern    string           `json:"artifact_pattern"`
	ArtifactSinceStart bool             `json:"artifact_since_start"`
	PollInterval       string           `json:"poll_interval"`
	Args               []string         `json:"args"`
	ConfigFile         string           `json:"config_file,omitempty"`
	Profile            string           `json:"profile,omitempty"`
	GitCommit          string           `json:"git_commit,omitempty"`
	GitBranch          string           `json:"git_branch,omitempty"`
}

type ArtifactSource struct {
	Path     string   `json:"path"`
	Patterns []string `json:"artifact_patterns"`
}

func main() {
	if len(os.Args) < 2 {
		printUsage()
		return
	}

	switch os.Args[1] {
	case "run":
		if err := cmdRun(os.Args[2:]); err != nil {
			log.Fatalf("exp run: %v", err)
		}
	case "list":
		if err := cmdList(os.Args[2:]); err != nil {
			log.Fatalf("exp list: %v", err)
		}
	case "show":
		if err := cmdShow(os.Args[2:]); err != nil {
			log.Fatalf("exp show: %v", err)
		}
	case "fetch":
		if err := cmdFetch(os.Args[2:]); err != nil {
			log.Fatalf("exp fetch: %v", err)
		}
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println(`Usage:
  exp run   [flags] -- [remote script args...]
  exp list
  exp show  <id>
  exp fetch <id> [flags]

Commands:
  run   Submit an experiment via ssh + sbatch on remote host and record it locally.
  list  List recorded experiments (stored locally).
  show  Show details of one experiment by ID.
  fetch Download experiment artifacts from the remote host via rsync.

 Examples:
  exp run \
    --remote baidya.ar@explorer-01 \
    --name bigann-k100-bw8 \
    --log-dir /projects/SaltSystemsLab/arunit/logs \
    --script /projects/SaltSystemsLab/arunit/scripts/query_bigann.sbatch \
    --artifact-remote /projects/SaltSystemsLab/arunit/gpu_ann/results/bigann \
    --artifact-dest ~/experiments/bigann-k100 \
    -- --k 100 --beam-width 8 --cut 10

  exp run --profile explorer --name bigann-k100-bw8 -- --k 100 --beam-width 8 --cut 10

  exp run --config-file explorer.yaml

  exp list

  exp show 1

  exp fetch 1 --remote-path /projects/foo/results --dest ./results --since-start --pattern 'json$'

 Notes:
  - --config-file accepts JSON or YAML describing a single run (name/remote/logs/artifacts/args).
  - Define defaults and profiles in ~/.exp/config.(json|yaml), then pass --profile NAME to avoid retyping remote/log/artifact paths.
  - Providing --artifact-remote/--artifact-dest makes "exp run" wait for completion and automatically rsync matching files.
  - exp fetch shells out to rsync locally and find on the remote host.
  - --remote-path must be an absolute path so rsync can address the files.`)
}

//
// DB helpers (local, on your laptop)
//

func dbPath() (string, error) {
	dir, err := configDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(dir, "experiments.db"), nil
}

func configDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".exp")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
}

func defaultConfigPaths() ([]string, error) {
	dir, err := configDir()
	if err != nil {
		return nil, err
	}
	return []string{
		filepath.Join(dir, "config.yaml"),
		filepath.Join(dir, "config.yml"),
		filepath.Join(dir, "config.json"),
	}, nil
}

func loadConfig() (*Config, error) {
	paths, err := defaultConfigPaths()
	if err != nil {
		return nil, err
	}
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		cfg := &Config{
			Profiles: make(map[string]RunProfile),
			path:     path,
		}
		if len(bytes.TrimSpace(data)) == 0 {
			return cfg, nil
		}
		if err := unmarshalConfigData(data, filepath.Ext(path), cfg); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
		if cfg.Profiles == nil {
			cfg.Profiles = make(map[string]RunProfile)
		}
		return cfg, nil
	}
	return nil, nil
}

func loadRunConfigFile(path string) (*RunConfigFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg RunConfigFile
	if err := unmarshalConfigData(data, filepath.Ext(path), &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &cfg, nil
}

func configPathHint() string {
	dir, err := configDir()
	if err != nil {
		return "~/.exp/config.(json|yaml)"
	}
	return fmt.Sprintf("%s/config.(json|yaml)", dir)
}

func openDB() (*sql.DB, error) {
	path, err := dbPath()
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if err := initSchema(db); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func initSchema(db *sql.DB) error {
	const createExperiments = `
CREATE TABLE IF NOT EXISTS experiments (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  name         TEXT,
  remote       TEXT,
  script_path  TEXT,
  args         TEXT,
  git_commit   TEXT,
  git_branch   TEXT,
  job_id       TEXT,
  job_status   TEXT,
  log_path     TEXT,
  created_at   TEXT,
  completed_at TEXT,
  artifact_remote TEXT,
  artifact_dest   TEXT,
  artifact_pattern TEXT,
  artifact_since_start INTEGER,
  artifact_last_sync   TEXT,
  artifact_last_error  TEXT,
  config_snapshot      TEXT
);`
	if _, err := db.Exec(createExperiments); err != nil {
		return err
	}
	migrations := []string{
		`ALTER TABLE experiments ADD COLUMN job_status TEXT`,
		`ALTER TABLE experiments ADD COLUMN completed_at TEXT`,
		`ALTER TABLE experiments ADD COLUMN artifact_remote TEXT`,
		`ALTER TABLE experiments ADD COLUMN artifact_dest TEXT`,
		`ALTER TABLE experiments ADD COLUMN artifact_pattern TEXT`,
		`ALTER TABLE experiments ADD COLUMN artifact_since_start INTEGER`,
		`ALTER TABLE experiments ADD COLUMN artifact_last_sync TEXT`,
		`ALTER TABLE experiments ADD COLUMN artifact_last_error TEXT`,
		`ALTER TABLE experiments ADD COLUMN config_snapshot TEXT`,
	}
	for _, stmt := range migrations {
		if _, err := db.Exec(stmt); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
				continue
			}
			return err
		}
	}
	return nil
}

func loadExperimentByID(db *sql.DB, id string) (*Experiment, error) {
	row := db.QueryRow(`SELECT id, name, remote, script_path, args, git_commit, git_branch, job_id, job_status, log_path,
                               created_at, completed_at, artifact_remote, artifact_dest, artifact_pattern,
                               artifact_since_start, artifact_last_sync, artifact_last_error, config_snapshot
                        FROM experiments WHERE id = ?`, id)

	var exp Experiment
	var created, completed, lastSync sql.NullString
	var sinceStart sql.NullInt64
	if err := row.Scan(
		&exp.ID,
		&exp.Name,
		&exp.Remote,
		&exp.ScriptPath,
		&exp.Args,
		&exp.GitCommit,
		&exp.GitBranch,
		&exp.JobID,
		&exp.JobStatus,
		&exp.LogPath,
		&created,
		&completed,
		&exp.ArtifactRemote,
		&exp.ArtifactDest,
		&exp.ArtifactPattern,
		&sinceStart,
		&lastSync,
		&exp.ArtifactLastError,
		&exp.ConfigSnapshot,
	); err != nil {
		return nil, err
	}
	if created.Valid && created.String != "" {
		if t, err := time.Parse(time.RFC3339, created.String); err == nil {
			exp.CreatedAt = t
		}
	}
	if completed.Valid && completed.String != "" {
		if t, err := time.Parse(time.RFC3339, completed.String); err == nil {
			exp.CompletedAt = t
		}
	}
	if sinceStart.Valid {
		exp.ArtifactSinceStart = sinceStart.Int64 == 1
	}
	if lastSync.Valid && lastSync.String != "" {
		if t, err := time.Parse(time.RFC3339, lastSync.String); err == nil {
			exp.ArtifactLastSync = t
		}
	}
	if exp.ConfigSnapshot != "" {
		var snap RunSnapshot
		if err := json.Unmarshal([]byte(exp.ConfigSnapshot), &snap); err == nil {
			if len(snap.ArtifactSources) > 0 {
				exp.ArtifactSources = copyArtifactSources(snap.ArtifactSources)
			}
		}
	}
	return &exp, nil
}

//
// git helpers (local repo info)
//

func getGitInfo() (commit, branch string) {
	c1 := exec.Command("git", "rev-parse", "HEAD")
	if out, err := c1.Output(); err == nil {
		commit = strings.TrimSpace(string(out))
	}
	c2 := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD")
	if out, err := c2.Output(); err == nil {
		branch = strings.TrimSpace(string(out))
	}
	return
}

func getRemoteGitInfo(remote, gitDir string) (commit, branch string, err error) {
	if remote == "" {
		return "", "", fmt.Errorf("remote host is required for remote git lookup")
	}
	if gitDir == "" {
		return "", "", fmt.Errorf("git directory is required for remote git lookup")
	}
	fmt.Printf("Running remote git commands in %s:%s\n", remote, gitDir)
	run := func(gitCmd string) (string, error) {
		cmdStr := fmt.Sprintf("hostname >&2 && cd %s && env GIT_DISCOVERY_ACROSS_FILESYSTEM=1 %s", shellQuote(gitDir), gitCmd)
		fmt.Printf("  ssh %s \"bash -lc %s\"\n", remote, shellQuote(cmdStr))
		cmd := exec.Command("ssh", remote, "bash", "-lc", cmdStr)
		var stdoutBuf, stderrBuf bytes.Buffer
		cmd.Stdout = &stdoutBuf
		cmd.Stderr = &stderrBuf
		err := cmd.Run()
		if stderrBuf.Len() > 0 {
			fmt.Print(stderrBuf.String())
		}
		if err != nil {
			return "", fmt.Errorf("%s: %v (stdout: %s stderr: %s)", gitCmd, err,
				strings.TrimSpace(stdoutBuf.String()), strings.TrimSpace(stderrBuf.String()))
		}
		return strings.TrimSpace(stdoutBuf.String()), nil
	}
	commit, err = run("git rev-parse HEAD")
	if err != nil {
		return "", "", err
	}
	fmt.Printf("Remote git commit from %s:%s = %s\n", remote, gitDir, commit)
	branch, err = run("git rev-parse --abbrev-ref HEAD")
	if err != nil {
		return "", "", err
	}
	fmt.Printf("Remote git branch from %s:%s = %s\n", remote, gitDir, branch)
	return commit, branch, nil
}

func uploadScript(remote, localPath, remotePath string) error {
	if remote == "" {
		return fmt.Errorf("remote host is required to upload script")
	}
	if localPath == "" {
		return fmt.Errorf("local script path is empty")
	}
	if remotePath == "" {
		return fmt.Errorf("remote script path (--script) is required when using --script-local")
	}
	absLocal, err := filepath.Abs(localPath)
	if err != nil {
		return fmt.Errorf("resolve script-local path: %w", err)
	}
	if _, err := os.Stat(absLocal); err != nil {
		return fmt.Errorf("script-local %s: %w", absLocal, err)
	}
	fmt.Printf("Uploading local script %s to %s:%s\n", absLocal, remote, remotePath)
	target := fmt.Sprintf("%s:%s", remote, remotePath)
	cmd := exec.Command("scp", absLocal, target)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("scp script to %s: %w", target, err)
	}
	return nil
}

func runRemoteBuildScript(remote, localPath string) error {
	if remote == "" {
		return fmt.Errorf("remote host is required for build script execution")
	}
	if localPath == "" {
		return fmt.Errorf("build script path is empty")
	}
	absLocal, err := expandLocalPath(localPath)
	if err != nil {
		return fmt.Errorf("build-script %s: %w", localPath, err)
	}
	info, err := os.Stat(absLocal)
	if err != nil {
		return fmt.Errorf("build-script %s: %w", absLocal, err)
	}
	if info.IsDir() {
		return fmt.Errorf("build-script %s is a directory", absLocal)
	}
	data, err := os.ReadFile(absLocal)
	if err != nil {
		return fmt.Errorf("read build-script %s: %w", absLocal, err)
	}
	if len(data) == 0 || data[len(data)-1] != '\n' {
		data = append(data, '\n')
	}
	remotePath := fmt.Sprintf("/tmp/exp-build-%d-%d.sh", time.Now().UnixNano(), os.Getpid())
	remoteQuoted := shellQuote(remotePath)
	marker := fmt.Sprintf("EXP_BUILD_%d", time.Now().UnixNano())
	fmt.Printf("Uploading and executing build script %s on %s:%s\n", absLocal, remote, remotePath)
	var builder strings.Builder
	builder.WriteString("set -eo pipefail; ")
	fmt.Fprintf(&builder, "cat > %s <<'%s'\n", remoteQuoted, marker)
	builder.Write(data)
	fmt.Fprintf(&builder, "%s\n", marker)
	fmt.Fprintf(&builder, "chmod +x %s\n", remoteQuoted)
	fmt.Fprintf(&builder, "bash %s\n", remoteQuoted)
	fmt.Fprintf(&builder, "rm -f %s\n", remoteQuoted)
	cmd := exec.Command("ssh", remote, "bash", "-lc", builder.String())
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("remote build script failed: %w", err)
	}
	return nil
}

//
// ssh + sbatch helper
//

// submitSbatchSSH runs sbatch on the remote host via SSH.
// remote: "user@host"
// logTemplate, scriptPath, scriptArgs must be valid paths/args on the remote machine.
func submitSbatchSSH(remote, logTemplate, scriptPath string, scriptArgs []string) (jobID string, sshOutput string, err error) {
	// ssh remote sbatch --output=logTemplate scriptPath [scriptArgs...]
	args := []string{
		remote,
		"sbatch",
		fmt.Sprintf("--output=%s", logTemplate),
		scriptPath,
	}
	args = append(args, scriptArgs...)

	cmd := exec.Command("ssh", args...)
	out, err := cmd.CombinedOutput()
	sshOutput = string(out)
	if err != nil {
		return "", sshOutput, fmt.Errorf("ssh/sbatch failed: %v\nOutput: %s", err, sshOutput)
	}

	// Typical sbatch output: "Submitted batch job 2723147"
	parts := strings.Fields(sshOutput)
	if len(parts) == 0 {
		return "", sshOutput, fmt.Errorf("unable to parse sbatch output: %q", sshOutput)
	}
	jobID = parts[len(parts)-1]
	return jobID, sshOutput, nil
}

//
// Commands
//

// exp run --remote user@host --name NAME --log-dir REMOTE_DIR --script REMOTE_PATH -- [script-args...]
func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	var (
		remote           string
		name             string
		logDir           string
		script           string
		buildScript      string
		scriptLocal      string
		artifactRemote   string
		artifactDest     string
		artifactPatterns multiStringFlag
		configPath       string
		profileName      string
	)
	var configPatterns []string
	var artifactSources []ArtifactSource
	fs.StringVar(&remote, "remote", "", "Remote user@host for SSH (required)")
	fs.StringVar(&name, "name", "", "Logical name for the experiment (required)")
	fs.StringVar(&logDir, "log-dir", "", "REMOTE directory for sbatch logs (required)")
	fs.StringVar(&script, "script", "", "REMOTE path to sbatch script to run (required)")
	fs.StringVar(&buildScript, "build-script", "", "LOCAL path to a script executed on the remote host before submitting the job")
	fs.StringVar(&scriptLocal, "script-local", "", "LOCAL path to sbatch script to upload to --script before running (optional)")
	fs.StringVar(&artifactRemote, "artifact-remote", "", "REMOTE directory tree to sync after the job completes (optional)")
	fs.StringVar(&artifactDest, "artifact-dest", "", "LOCAL directory to store downloaded artifacts (optional)")
	fs.Var(&artifactPatterns, "artifact-pattern", "Regex filter applied to full remote artifact paths; may be repeated")
	fs.StringVar(&configPath, "config-file", "", "Path to YAML/JSON file describing this run (optional)")
	fs.StringVar(&profileName, "profile", "", "Profile name defined in ~/.exp/config.json to use as defaults")

	artifactSinceStartFlag := boolFlag{value: true}
	fs.Var(&artifactSinceStartFlag, "artifact-since-start", "Only copy files newer than experiment start when syncing artifacts")

	pollIntervalFlag := durationFlag{value: defaultPollInterval}
	fs.Var(&pollIntervalFlag, "poll-interval", "How frequently to poll job status (e.g. 45s, 2m)")

	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: exp run --remote user@host --name NAME --log-dir REMOTE_DIR --script REMOTE_PATH -- [script-args...]\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	artifactSinceStart := artifactSinceStartFlag.value
	pollInterval := pollIntervalFlag.value

	var runFile *RunConfigFile
	if configPath != "" {
		absPath, err := filepath.Abs(configPath)
		if err != nil {
			return fmt.Errorf("config-file: %w", err)
		}
		cfg, err := loadRunConfigFile(absPath)
		if err != nil {
			return err
		}
		runFile = cfg
		configPath = absPath
		if profileName == "" && runFile.Profile != "" {
			profileName = runFile.Profile
		}
		if name == "" && runFile.Name != "" {
			name = runFile.Name
		}
	}

	cfg, err := loadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	applyProfile := func(source string, prof *RunProfile) error {
		if prof == nil {
			return nil
		}
		if remote == "" {
			remote = prof.Remote
		}
		if logDir == "" {
			logDir = prof.LogDir
		}
		if script == "" {
			script = prof.Script
		}
		if buildScript == "" {
			buildScript = prof.BuildScript
		}
		if artifactRemote == "" {
			artifactRemote = prof.ArtifactRemote
		}
		if artifactDest == "" {
			artifactDest = prof.ArtifactDest
		}
		if len(configPatterns) == 0 {
			if patterns := normalizePatternList(prof.ArtifactPattern, prof.ArtifactPatterns); len(patterns) > 0 {
				configPatterns = patterns
			}
		}
		if len(artifactSources) == 0 && len(prof.ArtifactSources) > 0 {
			artifactSources = copyArtifactSources(prof.ArtifactSources)
		}
		if !artifactSinceStartFlag.set && prof.ArtifactSinceStart != nil {
			artifactSinceStart = *prof.ArtifactSinceStart
		}
		if !pollIntervalFlag.set && prof.PollInterval != "" {
			d, err := time.ParseDuration(prof.PollInterval)
			if err != nil {
				return fmt.Errorf("invalid poll_interval %q in profile %s: %w", prof.PollInterval, source, err)
			}
			pollInterval = d
		}
		return nil
	}
	if cfg == nil {
		if profileName != "" {
			return fmt.Errorf("profile %q requested but no config file found (expected %s)", profileName, configPathHint())
		}
	} else {
		if err := applyProfile("defaults", &cfg.Defaults); err != nil {
			return err
		}
		if profileName != "" {
			prof, ok := cfg.Profiles[profileName]
			if !ok {
				return fmt.Errorf("profile %q not found in %s", profileName, cfg.path)
			}
			if err := applyProfile(profileName, &prof); err != nil {
				return err
			}
		}
	}

	applyRunFile := func(source string, cfg *RunConfigFile) error {
		if cfg == nil {
			return nil
		}
		if remote == "" {
			remote = cfg.Remote
		}
		if logDir == "" {
			logDir = cfg.LogDir
		}
		if script == "" {
			script = cfg.Script
		}
		if buildScript == "" {
			buildScript = cfg.BuildScript
		}
		if scriptLocal == "" {
			scriptLocal = cfg.ScriptLocal
		}
		if artifactRemote == "" {
			artifactRemote = cfg.ArtifactRemote
		}
		if artifactDest == "" {
			artifactDest = cfg.ArtifactDest
		}
		if len(artifactSources) == 0 && len(cfg.ArtifactSources) > 0 {
			artifactSources = copyArtifactSources(cfg.ArtifactSources)
		}
		if len(configPatterns) == 0 {
			if patterns := normalizePatternList(cfg.ArtifactPattern, cfg.ArtifactPatterns); len(patterns) > 0 {
				configPatterns = patterns
			}
		}
		if !artifactSinceStartFlag.set && cfg.ArtifactSinceStart != nil {
			artifactSinceStart = *cfg.ArtifactSinceStart
		}
		if !pollIntervalFlag.set && cfg.PollInterval != "" {
			d, err := time.ParseDuration(cfg.PollInterval)
			if err != nil {
				return fmt.Errorf("invalid poll_interval %q in %s: %w", cfg.PollInterval, source, err)
			}
			pollInterval = d
		}
		if name == "" {
			name = cfg.Name
		}
		return nil
	}
	if err := applyRunFile(configPath, runFile); err != nil {
		return err
	}

	if remote == "" {
		// allow ENV override
		remote = os.Getenv("EXP_REMOTE")
	}
	if remote == "" || name == "" || logDir == "" || script == "" {
		fs.Usage()
		return fmt.Errorf("remote, name, log-dir, and script are required (or set EXP_REMOTE)")
	}

	if artifactRemote != "" && !strings.HasPrefix(artifactRemote, "/") {
		return fmt.Errorf("artifact-remote must be an absolute path on the remote host")
	}
	if len(artifactSources) == 0 && (artifactRemote == "") != (artifactDest == "") {
		return fmt.Errorf("artifact-remote and artifact-dest must be provided together (or specify artifact-sources)")
	}
	patterns := configPatterns
	if vals := artifactPatterns.Values(); len(vals) > 0 {
		patterns = vals
	}
	artifactDestAbs := ""
	if artifactDest != "" {
		var err error
		artifactDestAbs, err = expandLocalPath(artifactDest)
		if err != nil {
			return fmt.Errorf("artifact-dest: %w", err)
		}
		if err := os.MkdirAll(artifactDestAbs, 0o755); err != nil {
			return fmt.Errorf("ensure artifact destination %s: %w", artifactDestAbs, err)
		}
	}
	if buildScript != "" {
		if remote == "" {
			return fmt.Errorf("build-script requires a remote host")
		}
		if err := runRemoteBuildScript(remote, buildScript); err != nil {
			return err
		}
	}
	if scriptLocal != "" {
		if err := uploadScript(remote, scriptLocal, script); err != nil {
			return err
		}
	}
	sources := copyArtifactSources(artifactSources)
	if len(sources) == 0 && artifactRemote != "" {
		sources = []ArtifactSource{{
			Path:     artifactRemote,
			Patterns: ensurePatterns(patterns),
		}}
	}
	if artifactDestAbs != "" && len(sources) == 0 {
		return fmt.Errorf("artifact sources are required when artifact-dest is set")
	}
	for i := range sources {
		if sources[i].Path == "" {
			return fmt.Errorf("artifact source %d has empty path", i+1)
		}
		if !strings.HasPrefix(sources[i].Path, "/") {
			return fmt.Errorf("artifact source path %q must be absolute", sources[i].Path)
		}
		sources[i].Patterns = ensurePatterns(sources[i].Patterns)
	}
	artifactPatternCombined := combinePatterns(flattenPatternsFromSources(sources))
	if artifactPatternCombined == "" {
		artifactPatternCombined = combinePatterns(patterns)
	}
	primaryRemote := artifactRemote
	if len(sources) > 0 {
		primaryRemote = sources[0].Path
	}

	// Script args are whatever remains after flags.
	scriptArgs := fs.Args()
	if len(scriptArgs) == 0 && runFile != nil && len(runFile.Args) > 0 {
		scriptArgs = append([]string(nil), runFile.Args...)
	}

	// Remote log template, include %j for the job id (interpreted by sbatch on remote).
	logTemplate := filepath.Join(logDir, fmt.Sprintf("%s-%%j.out", name))

	// Submit via ssh + sbatch.
	jobID, sshOut, err := submitSbatchSSH(remote, logTemplate, script, scriptArgs)
	if err != nil {
		return err
	}

	// Final remote log path (with job id substituted).
	logPath := strings.ReplaceAll(logTemplate, "%j", jobID)

	gitDirs := []string{}
	if script != "" {
		if dir := filepath.Dir(script); dir != "" && dir != "." {
			gitDirs = append(gitDirs, dir)
		}
	}
	if artifactRemote != "" {
		gitDirs = append(gitDirs, artifactRemote)
	}

	// Remote git info (try script dir, then artifact remote); fall back to local if unavailable.
	commit, branch := "", ""
	for _, dir := range gitDirs {
		if dir == "" {
			continue
		}
		fmt.Printf("Attempting remote git lookup at %s:%s\n", remote, dir)
		if c, b, err := getRemoteGitInfo(remote, dir); err == nil {
			commit, branch = c, b
			fmt.Printf("Remote git lookup succeeded at %s:%s (commit=%s branch=%s)\n", remote, dir, commit, branch)
			break
		} else {
			fmt.Printf("Warning: unable to read remote git info from %s: %v\n", dir, err)
		}
	}
	if commit == "" && branch == "" {
		fmt.Println("Warning: unable to determine remote git directory; recording local git metadata")
		commit, branch = getGitInfo()
	}

	snapshot := RunSnapshot{
		Name:               name,
		Remote:             remote,
		LogDir:             logDir,
		Script:             script,
		BuildScript:        buildScript,
		ArtifactPatterns:   append([]string(nil), patterns...),
		ArtifactRemote:     artifactRemote,
		ArtifactDest:       artifactDestAbs,
		ArtifactPattern:    artifactPatternCombined,
		ArtifactSinceStart: artifactSinceStart,
		PollInterval:       pollInterval.String(),
		Args:               append([]string(nil), scriptArgs...),
		Profile:            profileName,
		GitCommit:          commit,
		GitBranch:          branch,
	}
	if configPath != "" {
		snapshot.ConfigFile = configPath
	}

	// Save experiment locally.
	db, err := openDB()
	if err != nil {
		return fmt.Errorf("open DB: %w", err)
	}
	defer db.Close()

	snapshotBytes, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal config snapshot: %w", err)
	}
	snapshotJSON := string(snapshotBytes)

	createdAt := time.Now().UTC()
	now := createdAt.Format(time.RFC3339)

	res, err := db.Exec(
		`INSERT INTO experiments (name, remote, script_path, args, git_commit, git_branch, job_id, job_status, log_path,
                                  created_at, completed_at, artifact_remote, artifact_dest, artifact_pattern,
                                  artifact_since_start, artifact_last_sync, artifact_last_error, config_snapshot)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		name, remote, script, strings.Join(scriptArgs, " "), commit, branch, jobID, "SUBMITTED", logPath,
		now, "", primaryRemote, artifactDestAbs, artifactPatternCombined, boolToInt(artifactSinceStart), "", "", snapshotJSON,
	)
	if err != nil {
		return fmt.Errorf("insert experiment: %w", err)
	}
	id, _ := res.LastInsertId()
	artifactDestFinal := artifactDestAbs
	if artifactDestAbs != "" {
		artifactDestFinal = filepath.Join(artifactDestAbs, fmt.Sprintf("%d", id))
		if err := os.MkdirAll(artifactDestFinal, 0o755); err != nil {
			return fmt.Errorf("ensure artifact destination %s: %w", artifactDestFinal, err)
		}
		snapshot.ArtifactDest = artifactDestFinal
		snapshotBytes, err := json.Marshal(snapshot)
		if err != nil {
			return fmt.Errorf("marshal config snapshot: %w", err)
		}
		snapshotJSON = string(snapshotBytes)
		if _, err := db.Exec(`UPDATE experiments SET artifact_dest = ?, config_snapshot = ? WHERE id = ?`,
			artifactDestFinal, snapshotJSON, id); err != nil {
			return fmt.Errorf("update experiment destination: %w", err)
		}
	}

	fmt.Printf("Remote:       %s\n", remote)
	fmt.Printf("Submitted job %s via ssh\n", jobID)
	fmt.Printf("ssh/sbatch output: %s\n", strings.TrimSpace(sshOut))
	fmt.Printf("Recorded experiment %d locally\n", id)
	fmt.Printf("Remote log will be at: %s\n", logPath)
	if len(sources) > 0 {
		fmt.Printf("Artifacts:    %s -> %s\n", sources[0].Path, artifactDestFinal)
	}

	exp := &Experiment{
		ID:                 id,
		Name:               name,
		Remote:             remote,
		ScriptPath:         script,
		Args:               strings.Join(scriptArgs, " "),
		GitCommit:          commit,
		GitBranch:          branch,
		JobID:              jobID,
		JobStatus:          "SUBMITTED",
		LogPath:            logPath,
		CreatedAt:          createdAt,
		ArtifactRemote:     primaryRemote,
		ArtifactDest:       artifactDestFinal,
		ArtifactSources:    copyArtifactSources(sources),
		ArtifactPattern:    artifactPatternCombined,
		ArtifactSinceStart: artifactSinceStart,
		ConfigSnapshot:     snapshotJSON,
	}

	fmt.Printf("Monitoring job %s every %s ...\n", jobID, pollInterval)
	if err := monitorExperiment(db, exp, pollInterval); err != nil {
		return err
	}
	return nil
}

func cmdList(args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: exp list\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	db, err := openDB()
	if err != nil {
		return fmt.Errorf("open DB: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(`SELECT id, name, remote, job_id, job_status, created_at FROM experiments ORDER BY created_at DESC`)
	if err != nil {
		return fmt.Errorf("query experiments: %w", err)
	}
	defer rows.Close()

	fmt.Printf("%-5s %-25s %-22s %-10s %-12s %-20s\n", "ID", "NAME", "REMOTE", "JOB_ID", "STATUS", "CREATED_AT")
	for rows.Next() {
		var id int64
		var name, remote, jobID, status, created string
		if err := rows.Scan(&id, &name, &remote, &jobID, &status, &created); err != nil {
			return err
		}
		fmt.Printf("%-5d %-25s %-22s %-10s %-12s %-20s\n", id, name, remote, jobID, status, created)
	}
	return rows.Err()
}

func cmdShow(args []string) error {
	fs := flag.NewFlagSet("show", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: exp show <id>\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("id is required")
	}
	idStr := fs.Arg(0)

	db, err := openDB()
	if err != nil {
		return fmt.Errorf("open DB: %w", err)
	}
	defer db.Close()

	exp, err := loadExperimentByID(db, idStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no experiment with id %s", idStr)
		}
		return err
	}

	fmt.Printf("Experiment %d\n", exp.ID)
	fmt.Println("-------------")
	fmt.Printf("Name:        %s\n", exp.Name)
	fmt.Printf("Remote:      %s\n", exp.Remote)
	fmt.Printf("Job ID:      %s\n", exp.JobID)
	fmt.Printf("Job status:  %s\n", exp.JobStatus)
	fmt.Printf("Script:      %s\n", exp.ScriptPath)
	fmt.Printf("Args:        %s\n", exp.Args)
	fmt.Printf("Git commit:  %s\n", exp.GitCommit)
	fmt.Printf("Git branch:  %s\n", exp.GitBranch)
	fmt.Printf("Remote log:  %s\n", exp.LogPath)
	if !exp.CreatedAt.IsZero() {
		fmt.Printf("Created at:  %s\n", exp.CreatedAt.Format(time.RFC3339))
	} else {
		fmt.Printf("Created at:  (unknown)\n")
	}
	if !exp.CompletedAt.IsZero() {
		fmt.Printf("Completed:   %s\n", exp.CompletedAt.Format(time.RFC3339))
	}
	if exp.ArtifactRemote != "" {
		fmt.Printf("Artifacts\n")
		fmt.Printf("  Remote:    %s\n", exp.ArtifactRemote)
		fmt.Printf("  Dest:      %s\n", exp.ArtifactDest)
		if exp.ArtifactPattern != "" {
			patts := splitPatterns(exp.ArtifactPattern)
			if len(patts) == 0 {
				fmt.Printf("  Pattern:   (none)\n")
			} else if len(patts) == 1 {
				fmt.Printf("  Pattern:   %s\n", patts[0])
			} else {
				fmt.Println("  Patterns:")
				for _, p := range patts {
					fmt.Printf("    - %s\n", p)
				}
			}
		} else {
			fmt.Printf("  Pattern:   (none)\n")
		}
		fmt.Printf("  Since start filter: %t\n", exp.ArtifactSinceStart)
		if !exp.ArtifactLastSync.IsZero() {
			fmt.Printf("  Last sync: %s\n", exp.ArtifactLastSync.Format(time.RFC3339))
		}
		if exp.ArtifactLastError != "" {
			fmt.Printf("  Last error: %s\n", exp.ArtifactLastError)
		}
	}
	if exp.ConfigSnapshot != "" {
		fmt.Println("Config snapshot:")
		var pretty bytes.Buffer
		if err := json.Indent(&pretty, []byte(exp.ConfigSnapshot), "  ", "  "); err == nil {
			fmt.Println(pretty.String())
		} else {
			fmt.Println(exp.ConfigSnapshot)
		}
	}
	return nil
}

func cmdFetch(args []string) error {
	fs := flag.NewFlagSet("fetch", flag.ExitOnError)
	var (
		remotePath  string
		destDir     string
		patternFlag multiStringFlag
		dryRun      bool
	)
	var sinceStartFlag boolFlag
	fs.StringVar(&remotePath, "remote-path", "", "Absolute remote directory/file tree to copy (defaults to recorded artifact path)")
	fs.StringVar(&destDir, "dest", "", "Local destination directory for fetched files (defaults to recorded artifact destination)")
	fs.Var(&patternFlag, "pattern", "Regex applied to full remote paths (defaults to recorded artifact patterns); may be repeated")
	fs.Var(&sinceStartFlag, "since-start", "Only include files newer than the experiment start time (defaults to recorded preference)")
	fs.BoolVar(&dryRun, "dry-run", false, "Only list files that would be copied")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: exp fetch <id> --remote-path REMOTE --dest LOCAL [--pattern REGEX] [--since-start] [--dry-run]\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() != 1 {
		fs.Usage()
		return fmt.Errorf("experiment id is required")
	}
	db, err := openDB()
	if err != nil {
		return fmt.Errorf("open DB: %w", err)
	}
	defer db.Close()

	idStr := fs.Arg(0)
	exp, err := loadExperimentByID(db, idStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return fmt.Errorf("no experiment with id %s", idStr)
		}
		return err
	}
	if exp.Remote == "" {
		return fmt.Errorf("experiment %s has empty remote host", idStr)
	}

	if remotePath != "" && !strings.HasPrefix(remotePath, "/") {
		return fmt.Errorf("remote-path must be absolute so rsync can address files precisely")
	}

	if destDir == "" {
		destDir = exp.ArtifactDest
	}
	if destDir == "" {
		return fmt.Errorf("dest is required and no artifact destination is recorded for experiment %s", idStr)
	}

	sinceStart := exp.ArtifactSinceStart
	if sinceStartFlag.set {
		sinceStart = sinceStartFlag.value
	}

	overridePatterns := patternFlag.Values()
	sources := exp.EffectiveArtifactSources()
	if remotePath != "" {
		if !strings.HasPrefix(remotePath, "/") {
			return fmt.Errorf("remote-path must be absolute so rsync can address files precisely")
		}
		sources = []ArtifactSource{{Path: remotePath, Patterns: splitPatterns(exp.ArtifactPattern)}}
	}
	if len(sources) == 0 {
		return fmt.Errorf("no artifact sources recorded for experiment %s; use --remote-path", idStr)
	}
	if len(overridePatterns) > 0 {
		for i := range sources {
			sources[i].Patterns = overridePatterns
		}
	}

	if err := fetchArtifactSources(exp, sources, destDir, sinceStart, dryRun); err != nil {
		if err2 := recordArtifactSync(db, exp.ID, nil, err.Error()); err2 != nil {
			return fmt.Errorf("%v (additionally failed to record sync state: %w)", err, err2)
		}
		return err
	}

	if dryRun {
		return nil
	}

	now := time.Now().UTC()
	if err := recordArtifactSync(db, exp.ID, &now, ""); err != nil {
		return err
	}
	fmt.Println("Fetch complete.")
	return nil
}

func monitorExperiment(db *sql.DB, exp *Experiment, interval time.Duration) error {
	fmt.Printf("Monitoring job %s on %s\n", exp.JobID, exp.Remote)
	for {
		status, err := queryJobStatus(exp.Remote, exp.JobID)
		if err != nil {
			fmt.Printf("Warning: unable to query job status: %v\n", err)
			time.Sleep(interval)
			continue
		}
		exp.JobStatus = status
		if err := updateExperimentStatus(db, exp.ID, status, nil); err != nil {
			return err
		}
		fmt.Printf("[%s] %s -> %s\n", time.Now().Format(time.RFC3339), exp.JobID, status)
		if !isActiveStatus(status) {
			completed := time.Now().UTC()
			exp.CompletedAt = completed
			if err := updateExperimentStatus(db, exp.ID, status, &completed); err != nil {
				return err
			}
			break
		}
		time.Sleep(interval)
	}

	sources := exp.EffectiveArtifactSources()
	if len(sources) > 0 && exp.ArtifactDest != "" {
		fmt.Println("Job finished; fetching artifacts from configured sources")
		time.Sleep(artifactSettleDelay)
		if err := fetchArtifactSources(exp, sources, exp.ArtifactDest, exp.ArtifactSinceStart, false); err != nil {
			fmt.Printf("Artifact sync failed: %v\n", err)
			if err := recordArtifactSync(db, exp.ID, nil, err.Error()); err != nil {
				return err
			}
			return err
		}
		now := time.Now().UTC()
		exp.ArtifactLastSync = now
		if err := recordArtifactSync(db, exp.ID, &now, ""); err != nil {
			return err
		}
		fmt.Printf("Artifacts stored under %s\n", exp.ArtifactDest)
	} else {
		fmt.Println("No artifact paths configured for this experiment; skipping automatic fetch.")
	}
	return nil
}

func isActiveStatus(status string) bool {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "PENDING", "CONFIGURING", "RUNNING", "COMPLETING", "SUSPENDED", "RESV_DEL_HOLD", "SPECIAL_EXIT":
		return true
	default:
		return false
	}
}

func queryJobStatus(remote, jobID string) (string, error) {
	if jobID == "" {
		return "UNKNOWN", nil
	}
	status, err := runSqueue(remote, jobID)
	if err != nil {
		return "", err
	}
	if status != "" {
		return status, nil
	}
	if status, err = runSacct(remote, jobID); err == nil {
		if status != "" {
			return status, nil
		}
	} else {
		// sacct is optional; treat missing sacct as "unknown" once the job leaves squeue
		return "UNKNOWN", nil
	}
	return "UNKNOWN", nil
}

func runSqueue(remote, jobID string) (string, error) {
	cmd := exec.Command("ssh", remote, "squeue", "-h", "-j", jobID, "-o", "%T")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("squeue: %v (output: %s)", err, strings.TrimSpace(string(out)))
	}
	text := strings.TrimSpace(string(out))
	if text == "" {
		return "", nil
	}
	lines := strings.Split(text, "\n")
	return strings.TrimSpace(lines[0]), nil
}

func runSacct(remote, jobID string) (string, error) {
	cmd := exec.Command("ssh", remote, "sacct", "-n", "-X", "-j", jobID, "-o", "State")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("sacct: %v (output: %s)", err, strings.TrimSpace(string(out)))
	}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		state := line
		if idx := strings.Index(state, " "); idx >= 0 {
			state = state[:idx]
		}
		state = strings.Trim(state, "+")
		return state, nil
	}
	return "", nil
}

func updateExperimentStatus(db *sql.DB, id int64, status string, completedAt *time.Time) error {
	if completedAt != nil {
		_, err := db.Exec(`UPDATE experiments SET job_status = ?, completed_at = ? WHERE id = ?`,
			status, completedAt.Format(time.RFC3339), id)
		return err
	}
	_, err := db.Exec(`UPDATE experiments SET job_status = ? WHERE id = ?`, status, id)
	return err
}

func recordArtifactSync(db *sql.DB, id int64, syncedAt *time.Time, errMsg string) error {
	ts := ""
	if syncedAt != nil {
		ts = syncedAt.Format(time.RFC3339)
	}
	_, err := db.Exec(`UPDATE experiments SET artifact_last_sync = ?, artifact_last_error = ? WHERE id = ?`, ts, errMsg, id)
	return err
}

func fetchArtifactSources(exp *Experiment, sources []ArtifactSource, destDir string, sinceStart bool, dryRun bool) error {
	if len(sources) == 0 {
		fmt.Println("No artifact sources to process; nothing to copy.")
		return nil
	}
	for _, src := range sources {
		if src.Path == "" {
			return fmt.Errorf("artifact source has empty path")
		}
		fmt.Printf("Fetching artifacts from %s\n", src.Path)
		if err := fetchArtifacts(exp, src.Path, destDir, ensurePatterns(src.Patterns), sinceStart, dryRun); err != nil {
			return err
		}
	}
	return nil
}

func fetchArtifacts(exp *Experiment, remotePath, destDir string, patterns []string, sinceStart bool, dryRun bool) error {
	if remotePath == "" {
		return fmt.Errorf("remote-path is required")
	}
	if !strings.HasPrefix(remotePath, "/") {
		return fmt.Errorf("remote-path must be absolute so rsync can address the files precisely")
	}
	absDest, err := expandLocalPath(destDir)
	if err != nil {
		return fmt.Errorf("artifact destination: %w", err)
	}
	if absDest == "" {
		return fmt.Errorf("destination directory is required")
	}

	var since time.Time
	if sinceStart {
		if exp.CreatedAt.IsZero() {
			return fmt.Errorf("experiment %d does not have a recorded start time, cannot apply since-start filter", exp.ID)
		}
		since = exp.CreatedAt
	}

	var files []string
	var cmd string

	attempts := []struct {
		label string
		ts    time.Time
	}{
		{"without time filter", time.Time{}},
	}
	if sinceStart {
		attempts = append([]struct {
			label string
			ts    time.Time
		}{{"since-start window", since}}, attempts...)
	}

	for _, attempt := range attempts {
		for retry := 0; retry < 6; retry++ {
			if retry > 0 {
				time.Sleep(3 * time.Second)
			}
			fmt.Printf("Querying %s for files under %s (%s, attempt %d)...\n", exp.Remote, remotePath, attempt.label, retry+1)
			files, cmd, err = listRemoteFiles(exp.Remote, remotePath, attempt.ts)
			fmt.Println("files found: ", files)
			if err != nil {
				return err
			}
			if len(files) > 0 {
				goto FILES_FOUND
			}
		}
	}

FILES_FOUND:
	if len(files) == 0 {
		fmt.Printf("Remote find produced no files (command: %s)\n", cmd)
		fmt.Println("No files matched the provided filters; nothing to copy.")
		return nil
	}

	var compiled []*regexp.Regexp
	for _, pat := range patterns {
		pat = strings.TrimSpace(pat)
		if pat == "" {
			continue
		}
		re, err := regexp.Compile(pat)
		if err != nil {
			return fmt.Errorf("compile pattern %q: %w", pat, err)
		}
		compiled = append(compiled, re)
	}

	var filtered []string
	for _, rel := range files {
		if !patternMatches(compiled, remotePath, rel) {
			continue
		}
		filtered = append(filtered, rel)
	}

	if len(filtered) == 0 {
		fmt.Println("No files matched the provided filters; nothing to copy.")
		return nil
	}

	fmt.Printf("Matched %d file(s).\n", len(filtered))
	if dryRun {
		for _, rel := range filtered {
			fmt.Println(filepath.Join(remotePath, rel))
		}
		return nil
	}

	if err := rsyncFiles(exp.Remote, remotePath, filtered, absDest); err != nil {
		return err
	}
	return nil
}

func listRemoteFiles(remote, root string, since time.Time) ([]string, string, error) {
	if cwd, err := os.Getwd(); err == nil {
		fmt.Printf("Local PWD during listRemoteFiles: %s\n", cwd)
	} else {
		fmt.Printf("Local PWD during listRemoteFiles: unable to determine working directory: %v\n", err)
	}
	var cmdBuilder strings.Builder
	fmt.Fprintf(&cmdBuilder, "echo Remote initial PWD: \"$PWD\" >&2 && cd %s && echo Remote PWD after cd: \"$PWD\" >&2 && find . -type f", shellQuote(root))
	if !since.IsZero() {
		cutoff := since.UTC().Add(-sinceStartGracePeriod)
		epoch := cutoff.Unix()
		fmt.Fprintf(&cmdBuilder, " -newermt %s", shellQuote(fmt.Sprintf("@%d", epoch)))
	}
	cmdBuilder.WriteString(" -print")

	cmd := exec.Command("ssh", remote, "bash", "-lc", cmdBuilder.String())
	var stdoutBuf, stderrBuf bytes.Buffer
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf
	err := cmd.Run()
	stderrText := stderrBuf.String()
	if stderrText != "" {
		fmt.Print(stderrText)
	}
	if err != nil {
		return nil, cmdBuilder.String(), fmt.Errorf("remote find failed: %v\nCommand: %s\nStdout: %s\nStderr: %s",
			err, cmdBuilder.String(), strings.TrimSpace(stdoutBuf.String()), strings.TrimSpace(stderrText))
	}
	trimmed := strings.TrimSpace(stdoutBuf.String())
	if trimmed == "" {
		return nil, cmdBuilder.String(), nil
	}
	lines := strings.Split(trimmed, "\n")
	var files []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || line == "." {
			continue
		}
		line = strings.TrimPrefix(line, "./")
		files = append(files, line)
	}
	return files, cmdBuilder.String(), nil
}

func rsyncFiles(remote, root string, files []string, dest string) error {
	if len(files) == 0 {
		return nil
	}
	absDest, err := filepath.Abs(dest)
	if err != nil {
		return fmt.Errorf("resolve destination: %w", err)
	}
	if err := os.MkdirAll(absDest, 0o755); err != nil {
		return fmt.Errorf("ensure destination: %w", err)
	}

	sourceRoot := strings.TrimRight(root, "/")
	if sourceRoot == "" {
		sourceRoot = "/"
	}
	src := fmt.Sprintf("%s:%s/", remote, sourceRoot)
	args := []string{"-av", "--files-from=-", src, absDest}
	cmd := exec.Command("rsync", args...)
	cmd.Stdin = strings.NewReader(strings.Join(files, "\n") + "\n")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	fmt.Printf("Starting rsync: rsync %s %s\n", strings.Join(args[:len(args)-2], " "), strings.Join(args[len(args)-2:], " "))
	fmt.Printf("  Files-from: %s\n  Destination: %s\n", src, absDest)
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("rsync failed: %w", err)
	}
	return nil
}

func patternMatches(res []*regexp.Regexp, remoteRoot, rel string) bool {
	if len(res) == 0 {
		return true
	}
	full := rel
	if remoteRoot != "" {
		full = filepath.Join(remoteRoot, rel)
	}
	base := filepath.Base(rel)
	for _, re := range res {
		if re == nil {
			continue
		}
		if re.MatchString(rel) || re.MatchString(base) || re.MatchString(full) {
			return true
		}
	}
	return false
}

func shellQuote(s string) string {
	if s == "" {
		return "''"
	}
	return "'" + strings.ReplaceAll(s, `'`, `'"'"'`) + "'"
}

func expandLocalPath(p string) (string, error) {
	if p == "" {
		return "", nil
	}
	if strings.HasPrefix(p, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		rest := strings.TrimPrefix(p, "~")
		if strings.HasPrefix(rest, "/") {
			rest = rest[1:]
		}
		if rest == "" {
			p = home
		} else {
			p = filepath.Join(home, rest)
		}
	}
	return filepath.Abs(p)
}

func copyArtifactSources(src []ArtifactSource) []ArtifactSource {
	if len(src) == 0 {
		return nil
	}
	dst := make([]ArtifactSource, len(src))
	for i, s := range src {
		dst[i] = ArtifactSource{
			Path:     s.Path,
			Patterns: append([]string(nil), s.Patterns...),
		}
	}
	return dst
}

func normalizePatternList(single string, list []string) []string {
	var res []string
	for _, p := range list {
		p = strings.TrimSpace(p)
		if p != "" {
			res = append(res, p)
		}
	}
	s := strings.TrimSpace(single)
	if s != "" {
		res = append(res, s)
	}
	return res
}

func ensurePatterns(pats []string) []string {
	if len(pats) == 0 {
		return nil
	}
	var out []string
	for _, p := range pats {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func combinePatterns(patterns []string) string {
	var cleaned []string
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p != "" {
			cleaned = append(cleaned, p)
		}
	}
	return strings.Join(cleaned, "\n")
}

func splitPatterns(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	lines := strings.Split(s, "\n")
	var out []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

func (exp *Experiment) EffectiveArtifactSources() []ArtifactSource {
	if exp == nil {
		return nil
	}
	if len(exp.ArtifactSources) > 0 {
		return copyArtifactSources(exp.ArtifactSources)
	}
	if exp.ArtifactRemote == "" {
		return nil
	}
	return []ArtifactSource{{
		Path:     exp.ArtifactRemote,
		Patterns: splitPatterns(exp.ArtifactPattern),
	}}
}

func flattenPatternsFromSources(src []ArtifactSource) []string {
	var out []string
	for _, s := range src {
		if len(s.Patterns) == 0 {
			out = append(out, "")
			continue
		}
		out = append(out, s.Patterns...)
	}
	return out
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

func unmarshalConfigData(data []byte, ext string, target interface{}) error {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return nil
	}
	format := strings.ToLower(ext)
	switch format {
	case ".yaml", ".yml":
		return unmarshalYAML(trimmed, target)
	case ".json", "":
		if err := json.Unmarshal(trimmed, target); err == nil {
			return nil
		}
		return unmarshalYAML(trimmed, target)
	default:
		if err := json.Unmarshal(trimmed, target); err == nil {
			return nil
		}
		return unmarshalYAML(trimmed, target)
	}
}

func unmarshalYAML(data []byte, target interface{}) error {
	node, err := parseYAMLDocument(data)
	if err != nil {
		return err
	}
	if node == nil {
		return nil
	}
	buf, err := json.Marshal(node)
	if err != nil {
		return err
	}
	if len(buf) == 0 {
		return nil
	}
	return json.Unmarshal(buf, target)
}

func parseYAMLDocument(data []byte) (map[string]interface{}, error) {
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	m, idx, err := parseYAMLMap(lines, 0, 0)
	if err != nil {
		return nil, err
	}
	for i := idx; i < len(lines); i++ {
		_, _, ok, err := preprocessYAMLLine(lines[i])
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		if !ok {
			continue
		}
		return nil, fmt.Errorf("line %d: unexpected content", i+1)
	}
	return m, nil
}

func parseYAMLMap(lines []string, start, indent int) (map[string]interface{}, int, error) {
	result := make(map[string]interface{})
	i := start
	for i < len(lines) {
		trimmed, lineIndent, ok, err := preprocessYAMLLine(lines[i])
		if err != nil {
			return nil, 0, fmt.Errorf("line %d: %w", i+1, err)
		}
		if !ok {
			i++
			continue
		}
		if lineIndent < indent {
			break
		}
		if lineIndent > indent {
			return nil, 0, fmt.Errorf("line %d: unexpected indentation", i+1)
		}
		if strings.HasPrefix(trimmed, "- ") {
			return nil, 0, fmt.Errorf("line %d: unexpected list item", i+1)
		}
		colon := strings.Index(trimmed, ":")
		if colon < 0 {
			return nil, 0, fmt.Errorf("line %d: expected ':'", i+1)
		}
		key := strings.TrimSpace(trimmed[:colon])
		if key == "" {
			return nil, 0, fmt.Errorf("line %d: missing key", i+1)
		}
		rest := strings.TrimSpace(trimmed[colon+1:])
		if rest != "" {
			result[key] = parseYAMLScalar(rest)
			i++
			continue
		}
		nextIdx := i + 1
		handled := false
		for nextIdx < len(lines) {
			nextTrimmed, nextIndent, ok, err := preprocessYAMLLine(lines[nextIdx])
			if err != nil {
				return nil, 0, fmt.Errorf("line %d: %w", nextIdx+1, err)
			}
			if !ok {
				nextIdx++
				continue
			}
			if nextIndent < indent+2 {
				result[key] = map[string]interface{}{}
				i = nextIdx
				handled = true
				break
			}
			if nextIndent > indent+2 {
				return nil, 0, fmt.Errorf("line %d: unexpected indentation", nextIdx+1)
			}
			if strings.HasPrefix(nextTrimmed, "- ") {
				list, newIdx, err := parseYAMLList(lines, nextIdx, indent+2)
				if err != nil {
					return nil, 0, err
				}
				result[key] = list
				i = newIdx
				handled = true
				break
			}
			child, newIdx, err := parseYAMLMap(lines, nextIdx, indent+2)
			if err != nil {
				return nil, 0, err
			}
			result[key] = child
			i = newIdx
			handled = true
			break
		}
		if !handled {
			result[key] = map[string]interface{}{}
			i = nextIdx
		}
	}
	return result, i, nil
}

func parseYAMLList(lines []string, start, indent int) ([]interface{}, int, error) {
	var items []interface{}
	i := start
	for i < len(lines) {
		trimmed, lineIndent, ok, err := preprocessYAMLLine(lines[i])
		if err != nil {
			return nil, 0, fmt.Errorf("line %d: %w", i+1, err)
		}
		if !ok {
			i++
			continue
		}
		if lineIndent < indent {
			break
		}
		if lineIndent > indent {
			return nil, 0, fmt.Errorf("line %d: unexpected indentation for list item", i+1)
		}
		if !strings.HasPrefix(trimmed, "-") {
			break
		}
		value := strings.TrimSpace(trimmed[1:])
		if value == "" {
			return nil, 0, fmt.Errorf("line %d: empty list items are not supported", i+1)
		}
		if isInlineMapListItem(value) {
			original := lines[i]
			lines[i] = strings.Repeat(" ", indent+2) + value
			child, newIdx, err := parseYAMLMap(lines, i, indent+2)
			lines[i] = original
			if err != nil {
				return nil, 0, err
			}
			items = append(items, child)
			i = newIdx
			continue
		}
		items = append(items, parseYAMLScalar(value))
		i++
	}
	return items, i, nil
}

func isInlineMapListItem(value string) bool {
	if len(value) == 0 {
		return false
	}
	switch value[0] {
	case '{', '[', '"', '\'':
		return false
	}
	colon := strings.Index(value, ":")
	if colon <= 0 {
		return false
	}
	key := strings.TrimSpace(value[:colon])
	if key == "" {
		return false
	}
	if strings.ContainsAny(key, " \t") {
		return false
	}
	if len(value) > colon+1 {
		if value[colon+1] != ' ' && value[colon+1] != '\t' {
			return false
		}
	}
	return true
}

func parseYAMLScalar(val string) interface{} {
	if len(val) == 0 {
		return ""
	}
	if val[0] == '"' || val[0] == '\'' {
		unquoted, err := strconv.Unquote(val)
		if err == nil {
			return unquoted
		}
	}
	lower := strings.ToLower(val)
	switch lower {
	case "true":
		return true
	case "false":
		return false
	case "null":
		return nil
	}
	if val[0] == '[' || val[0] == '{' {
		var v interface{}
		if json.Unmarshal([]byte(val), &v) == nil {
			return v
		}
	}
	return val
}

func preprocessYAMLLine(line string) (string, int, bool, error) {
	line = strings.TrimRight(line, "\r")
	line = stripInlineComment(line)
	trimmed := strings.TrimSpace(line)
	if trimmed == "" || trimmed == "---" || trimmed == "..." {
		return "", 0, false, nil
	}
	indent, err := countLeadingSpaces(line)
	if err != nil {
		return "", 0, false, err
	}
	return trimmed, indent, true, nil
}

func stripInlineComment(line string) string {
	var inSingle, inDouble bool
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '#':
			if !inSingle && !inDouble {
				return strings.TrimRight(line[:i], " \t")
			}
		}
	}
	return line
}

func countLeadingSpaces(line string) (int, error) {
	count := 0
	for i := 0; i < len(line); i++ {
		switch line[i] {
		case ' ':
			count++
		case '\t':
			return 0, fmt.Errorf("tabs are not supported in YAML input")
		default:
			return count, nil
		}
	}
	return count, nil
}
