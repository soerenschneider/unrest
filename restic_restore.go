package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/charmbracelet/huh"
	"github.com/charmbracelet/huh/spinner"
	"github.com/go-playground/validator/v10"
)

const (
	targetFilesystem            = "filesystem"
	targetSqlite                = "sqlite"
	targetMariaDb               = "mariadb"
	targetPostgres              = "postgres"
	resticRestoreSubpathDefault = "/"
)

var (
	validate                = validator.New()
	ErrMultipleEnvVarsFound = errors.New("found values for colliding environment variables")
)

type ResticSnapshot struct {
	Time     time.Time `json:"time"`
	Parent   string    `json:"parent"`
	Tree     string    `json:"tree"`
	Paths    []string  `json:"paths"`
	Hostname string    `json:"hostname"`
	Username string    `json:"username"`
	ID       string    `json:"id"`
	ShortID  string    `json:"short_id"`
	Tags     []string  `json:"tags"`
}

type ResticRestoreRequest struct {
	RestoreTarget string `validate:"required,oneof=sqlite mariadb postgres filesystem"`
	Repository    string `validate:"required"`
	Password      string `validate:"required"`

	SnapshotId         string `validate:"required"`
	SnapshotFilterHost string
	SnapshotFilterPath string
	SnapshotFilterTag  string

	SnapshotRestoreSubfolder string
	SnapshotDumpFile         string `validate:"required_unless=RestoreTarget filesystem,omitempty,filepath"`

	RestoreDryRun bool
	RestoreDelete bool
}

func (c *ResticRestoreRequest) GenerateSnapshotDataFromRequest() (ResticSnapshot, error) {
	shortId := c.SnapshotId
	if len(c.SnapshotId) >= 8 {
		shortId = c.SnapshotId[0:8]
	}

	ret := ResticSnapshot{
		Hostname: strings.Split(c.SnapshotFilterHost, ",")[0],
		ID:       c.SnapshotId,
		ShortID:  shortId,
		Tags:     strings.Split(c.SnapshotFilterTag, ","),
	}

	if c.RestoreTarget == targetFilesystem {
		if c.SnapshotRestoreSubfolder == "" {
			ret.Paths = []string{c.SnapshotRestoreSubfolder}
		} else {
			ret.Paths = []string{resticRestoreSubpathDefault}
		}
	} else {
		ret.Paths = []string{c.SnapshotDumpFile}
	}

	return ret, nil
}

func (c ResticRestoreRequest) String() string {
	var sb strings.Builder
	sb.WriteString("Restic:\n")
	sb.WriteString(fmt.Sprintf("  Snapshot ID: %s\n", c.SnapshotId))
	if c.SnapshotDumpFile != "" {
		sb.WriteString(fmt.Sprintf("  Snapshot Dump File: %s\n", c.SnapshotDumpFile))
	}

	// Only print filters if set
	if c.SnapshotFilterHost != "" {
		sb.WriteString(fmt.Sprintf("  Host Filter: %s\n", c.SnapshotFilterHost))
	}
	if c.SnapshotFilterPath != "" {
		sb.WriteString(fmt.Sprintf("  Path Filter: %s\n", c.SnapshotFilterPath))
	}
	if c.SnapshotFilterTag != "" {
		sb.WriteString(fmt.Sprintf("  Tag Filter: %s\n", c.SnapshotFilterTag))
	}

	if c.RestoreTarget == targetFilesystem {
		sb.WriteString(fmt.Sprintf("  Dry Run: %t\n", c.RestoreDryRun))
		sb.WriteString(fmt.Sprintf("  Delete Target: %t\n", c.RestoreDelete))
	}

	if c.SnapshotRestoreSubfolder != "" {
		sb.WriteString(fmt.Sprintf("  Restore Subfolder: %s\n", c.SnapshotRestoreSubfolder))
	}

	return sb.String()
}

type UnrestOptions struct {
	NoInteractive    bool
	SelectionTimeout time.Duration
	RestoreTimeout   time.Duration
}

type FilesystemParams struct {
	RestorePath string `validate:"required"`
}

func (c FilesystemParams) String() string {
	return fmt.Sprintf(
		"Filesystem:\n  restore-path: %s",
		c.RestorePath,
	)
}

type SqliteConnection struct {
	DatabasePath string `validate:"required,filepath"`
}

func (c SqliteConnection) String() string {
	return fmt.Sprintf(
		"Sqlite DB:\n  db-path: %s",
		c.DatabasePath,
	)
}

type MariadbConnection struct {
	DatabaseName  string `env:"MARIADB_DATABASE_NAME" validate:"omitempty"`
	User          string `validate:"required"`
	Pass          string `validate:"required"`
	Host          string `validate:"required"`
	Port          string `validate:"numeric"`
	ContainerName string
}

func (c MariadbConnection) String() string {
	return fmt.Sprintf(
		"Maria DB:\n  db-name: %s\n  user: %s\n  pass: %s\n  host: %s\n  port: %s\n  container: %s",
		c.DatabaseName,
		c.User,
		hideSensitive(c.Pass),
		c.Host,
		c.Port,
		c.ContainerName,
	)
}

// hideSensitive masks sensitive values
func hideSensitive(s string) string {
	if s == "" {
		return "<empty>"
	}
	return "<hidden>"
}

type PostgresConnection struct {
	DatabaseName  string
	User          string `validate:"required"`
	Pass          string `validate:"required"`
	Host          string `validate:"required"`
	Port          string `validate:"numeric"`
	ContainerName string
}

type ResticGetSnapshotsRequest struct {
	HostFilter []string
	PathFilter []string
	TagFilter  []string
}

func NewResticGetSnapshotsRequest(cfg ResticRestoreRequest) *ResticGetSnapshotsRequest {
	ret := &ResticGetSnapshotsRequest{}

	if cfg.SnapshotFilterHost != "" {
		ret.HostFilter = strings.Split(cfg.SnapshotFilterHost, ",")
	}

	if cfg.SnapshotFilterPath != "" {
		ret.PathFilter = strings.Split(cfg.SnapshotFilterPath, ",")
	}

	if cfg.SnapshotFilterTag != "" {
		ret.PathFilter = strings.Split(cfg.SnapshotFilterTag, ",")
	}

	return ret
}

func (c PostgresConnection) String() string {
	return fmt.Sprintf(
		"postgres:\n  db-name: %s\n  user: %s\n  pass: %s\n  host: %s\n  port: %s\n  container: %s",
		c.DatabaseName,
		c.User,
		hideSensitive(c.Pass),
		c.Host,
		c.Port,
		c.ContainerName,
	)
}

type cfg struct {
	Restic     ResticRestoreRequest
	Postgres   PostgresConnection
	Mariadb    MariadbConnection
	Sqlite     SqliteConnection
	Filesystem FilesystemParams
	AppOptions UnrestOptions
}

type unrest struct {
	resticSnapshots map[string]ResticSnapshot
	cfg             *cfg
}

func parseFlags(cfg *cfg) {
	flag.BoolVar(&cfg.AppOptions.NoInteractive, "restore-no-interactive", getBoolEnv("UNREST_NO_INTERACTIVE", false), "Do not ask for user-input.")
	flag.DurationVar(&cfg.AppOptions.SelectionTimeout, "selection-timeout", 5*time.Minute, "Time out waiting for user-input.")
	flag.DurationVar(&cfg.AppOptions.RestoreTimeout, "restore-timeout", 12*time.Hour, "Time out waiting for restore process to finish.")

	flag.StringVar(&cfg.Restic.Repository, "restic-repository", mustGetEnv("", "UNREST_RESTIC_REPO", "RESTIC_REPOSITORY"), "Target type to restore to. Possible values: filesystem, sqlite, mariadb, postgres.")
	flag.StringVar(&cfg.Restic.Password, "restic-password", mustGetEnv("", "UNREST_RESTIC_PASSWORD", "RESTIC_PASSWORD", "RESTIC_PASSWORD_FILE"), "Target type to restore to. Possible values: filesystem, sqlite, mariadb, postgres.")
	flag.StringVar(&cfg.Restic.RestoreTarget, "restic-target", os.Getenv("UNREST_RESTIC_TARGET"), "Target type to restore to. Possible values: filesystem, sqlite, mariadb, postgres.")
	flag.BoolVar(&cfg.Restic.RestoreDryRun, "restic-restore-dry-run", getBoolEnv("UNREST_RESTIC_DRY_RUN", false), "Dry run restore operation. Only effective when filesystem restore is selected")
	flag.BoolVar(&cfg.Restic.RestoreDelete, "restic-restore-delete", getBoolEnv("UNREST_RESTIC_DELETE", false), "Delete all files not in snapshot. Only effective when filesystem restore is selected")
	flag.StringVar(&cfg.Restic.SnapshotId, "restic-snapshot", os.Getenv("UNREST_RESTIC_SNAPSHOT_ID"), "ID of the snapshot to restore.")
	flag.StringVar(&cfg.Restic.SnapshotFilterHost, "restic-snapshot-filter-host", mustGetEnv("", "UNREST_RESTIC_SNAPSHOT_FILTER_HOST"), "Only display snapshots to chose from that match the host (comma-separated list)")
	flag.StringVar(&cfg.Restic.SnapshotFilterTag, "restic-snapshot-filter-tag", mustGetEnv("", "UNREST_RESTIC_SNAPSHOT_FILTER_TAG"), "Only display snapshots to chose from that match the tag (comma-separated list)")
	flag.StringVar(&cfg.Restic.SnapshotFilterPath, "restic-snapshot-filter-path", mustGetEnv("", "UNREST_RESTIC_SNAPSHOT_FILTER_PATH"), "Only display snapshots to chose from that match the path (comma-separated list)")

	flag.StringVar(&cfg.Restic.SnapshotDumpFile, "restic-dump-file", os.Getenv("UNREST_RESTIC_DUMP_FILE"), "File in the snapshot to stream to the restore command. If only one file is in the snapshot, this file is automatically picked. Only effective when restoring to either mariadb, postgres or sqlite.")

	flag.StringVar(&cfg.Restic.SnapshotRestoreSubfolder, "restic-restore-subfolder", mustGetEnv("", "UNREST_RESTIC_DUMP_FILE"), "Subfolder in the snapshot to restore. Only effective when filesystem restore is selected")

	// SQLite configuration
	flag.StringVar(&cfg.Sqlite.DatabasePath, "sqlite-db-path", mustGetEnv("", "UNREST_SQLITE_DB_PATH"), "Path to the SQLite database file. Example: /data/app.db")

	// Filesystem configuration
	flag.StringVar(&cfg.Filesystem.RestorePath, "fs-restore-path", mustGetEnv("", "UNREST_FS_RESTORE_PATH"), "Destination path for restoring filesystem backups. Example: /mnt/restore")

	// PostgreSQL configuration
	flag.StringVar(&cfg.Postgres.DatabaseName, "postgres-db-name", mustGetEnv("", "UNREST_PG_DB_NAME", "PGDATABASE", "POSTGRES_DATABASE_NAME"), "Name of the PostgreSQL database to connect to.")
	flag.StringVar(&cfg.Postgres.User, "postgres-user", mustGetEnv("", "UNREST_PG_USER", "PGUSER", "POSTGRES_USER"), "Username for PostgreSQL authentication.")
	flag.StringVar(&cfg.Postgres.Pass, "postgres-pass", mustGetEnv("", "UNREST_PG_PASS", "PGPASSWORD", "PGPASSFILE"), "Password for PostgreSQL authentication. Use environment variables or secret files when possible.")
	flag.StringVar(&cfg.Postgres.Host, "postgres-host", mustGetEnv("", "UNREST_PG_HOST", "PGHOST", "POSTGRES_HOST"), "Hostname or IP address of the PostgreSQL server.")
	flag.StringVar(&cfg.Postgres.Port, "postgres-port", mustGetEnv("5432", "UNREST_PG_PORT", "PGPORT"), "Port number for the PostgreSQL server. Default: 5432")
	flag.StringVar(&cfg.Postgres.ContainerName, "postgres-container", mustGetEnv("", "UNREST_PG_CONTAINER_NAME"), "Optional container name if PostgreSQL runs inside Docker or a similar environment.")

	// MariaDB configuration
	flag.StringVar(&cfg.Mariadb.DatabaseName, "mariadb-db-name", mustGetEnv("", "UNREST_MARIADB_DB_NAME"), "Name of the MariaDB database to connect to.")
	flag.StringVar(&cfg.Mariadb.User, "mariadb-user", mustGetEnv("", "UNREST_MARIADB_USER"), "Username for MariaDB authentication.")
	flag.StringVar(&cfg.Mariadb.Pass, "mariadb-pass", mustGetEnv("", "UNREST_MARIADB_PASS", "UNREST_MARIADB_PASS_FILE"), "Password for MariaDB authentication. Use environment variables or secret files when possible.")
	flag.StringVar(&cfg.Mariadb.Host, "mariadb-host", mustGetEnv("", "UNREST_MARIADB_HOST", "MYSQL_HOST"), "Hostname or IP address of the MariaDB server.")
	flag.StringVar(&cfg.Mariadb.Port, "mariadb-port", mustGetEnv("3306", "UNREST_MARIADB_PORT"), "Port number for the MariaDB server. Default: 3306")
	flag.StringVar(&cfg.Mariadb.ContainerName, "mariadb-container", mustGetEnv("", "UNREST_MARIADB_CONTAINER_NAME"), "Optional container name if MariaDB runs inside Docker or a similar environment.")

	flag.Usage = func() {
		// do not print the defaults
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		flag.VisitAll(func(f *flag.Flag) {
			// mimic Go's default indentation: 2 spaces before the flag
			fmt.Fprintf(os.Stderr, "  -%s\n\t%s\n", f.Name, f.Usage)
		})
	}

	flag.Parse()
}

func main() {
	app := &unrest{
		cfg: &cfg{},
	}

	parseFlags(app.cfg)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		if err := app.run(ctx); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-sigc
	cancel()
}

func (a *unrest) run(ctx context.Context) error {
	mandatoryConfigurationSupplied := true
	if err := a.validateConfig(); err != nil {
		if a.cfg.AppOptions.NoInteractive {
			return fmt.Errorf("failed to check for necessary info: %w", err)
		}
		mandatoryConfigurationSupplied = false
	}

	var err error
	var snapshot ResticSnapshot
	if mandatoryConfigurationSupplied {
		// create dummy snapshot data based on what the user supplied instead of loading real snapshot data
		a.resticSnapshots = make(map[string]ResticSnapshot)
		a.resticSnapshots[a.cfg.Restic.SnapshotId], err = a.cfg.Restic.GenerateSnapshotDataFromRequest()
		if err != nil {
			return fmt.Errorf("failed to generate snapshot data based on : %w", err)
		}
	} else {
		a.resticSnapshots, err = a.loadSnapshots(ctx)
		if err != nil {
			return err
		}

		if err := a.promptForMissingData(ctx, &a.cfg.Restic); err != nil {
			return fmt.Errorf("failed to gather data: %w", err)
		}

		var found bool
		snapshot, found = a.resticSnapshots[a.cfg.Restic.SnapshotId]
		if !found {
			return fmt.Errorf("snapshot for ID %q not found", a.cfg.Restic.SnapshotId)
		}
	}

	if a.cfg.Restic.RestoreTarget != targetFilesystem && len(snapshot.Paths) == 1 && a.cfg.Restic.SnapshotDumpFile == "" {
		a.cfg.Restic.SnapshotDumpFile = snapshot.Paths[0]
		fmt.Printf("Automatically selecting only file from snapshot: %q\n", a.cfg.Restic.SnapshotDumpFile)
	}

	restoreTarget, err := a.getBackupRestoreTarget(ctx)
	if err != nil {
		return fmt.Errorf("failed to get restore target implementation: %w", err)
	}

	return restoreTarget.restore(ctx, snapshot, a.cfg.Restic)
}

func (a *unrest) validateConfig() error {
	if err := validate.Struct(a.cfg); err != nil {
		validationErrors := err.(validator.ValidationErrors)
		fatalErrors := false
		for _, err := range validationErrors {
			if strings.HasPrefix(err.Namespace(), "cfg.Restic.") ||
				strings.HasPrefix(strings.ToLower(err.Namespace()), fmt.Sprintf("cfg.%s.", a.cfg.Restic.RestoreTarget)) {
				// only print missing config variables when we're not later asking for them interactively
				if a.cfg.AppOptions.NoInteractive {
					fmt.Println(err.Error())
				}
				fatalErrors = true
			}
		}
		if fatalErrors {
			return fmt.Errorf("failed to validate app options")
		}
	}

	return nil
}

// loadSnapshots loads snapshot data into the application by fetching them from an external source with optional interactivity.
func (a *unrest) loadSnapshots(ctx context.Context) (map[string]ResticSnapshot, error) {
	var snapshots []ResticSnapshot
	retrieveSnapshots := func(ctx context.Context) error {
		request := NewResticGetSnapshotsRequest(a.cfg.Restic)
		var err error
		snapshots, err = getSnapshots(ctx, *request)
		if err != nil {
			return fmt.Errorf("failed to get snapshots: %w", err)
		}
		return nil
	}

	if a.cfg.AppOptions.NoInteractive {
		if err := retrieveSnapshots(ctx); err != nil {
			return nil, fmt.Errorf("failed to get snapshots: %w", err)
		}
	} else {
		err := spinner.New().
			Context(ctx).
			Title("Loading snapshots...").
			ActionWithErr(func(context.Context) error {
				return retrieveSnapshots(ctx)
			}).
			Accessible(false).
			Run()
		if err != nil {
			return nil, fmt.Errorf("failed to get snapshots: %w", err)
		}
	}

	if len(snapshots) == 0 {
		return nil, fmt.Errorf("no snapshots found")
	}

	// synthetically add 'latest' snapshot
	resticSnapshots := make(map[string]ResticSnapshot, len(snapshots)+1)
	resticSnapshots["latest"] = snapshots[0]
	for _, s := range snapshots {
		resticSnapshots[s.ID] = s
	}

	return resticSnapshots, nil
}

func (a *unrest) getRestoreTargetConfig() (any, error) {
	switch a.cfg.Restic.RestoreTarget {
	case targetPostgres:
		return a.cfg.Postgres, nil
	case targetFilesystem:
		return a.cfg.Filesystem, nil
	case targetSqlite:
		return a.cfg.Sqlite, nil
	case targetMariaDb:
		return a.cfg.Mariadb, nil
	default:
		return nil, fmt.Errorf("unknown restore target type: %s", a.cfg.Restic.RestoreTarget)
	}
}

func getSnapshots(ctx context.Context, req ResticGetSnapshotsRequest) ([]ResticSnapshot, error) {
	args := []string{"snapshots", "--json"}
	if len(req.HostFilter) > 0 {
		for _, host := range req.HostFilter {
			args = append(args, "--host", host)
		}
	}

	if len(req.PathFilter) > 0 {
		for _, path := range req.PathFilter {
			args = append(args, "--path", path)
		}
	}

	if len(req.TagFilter) > 0 {
		for _, path := range req.TagFilter {
			args = append(args, "--tag", path)
		}
	}

	cmd := exec.CommandContext(ctx, "restic", args...)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute restic command: %w", err)
	}

	var snapshots []ResticSnapshot
	if err := json.Unmarshal(output, &snapshots); err != nil {
		return nil, fmt.Errorf("failed to parse snapshots: %w", err)
	}

	return snapshots, nil
}

func (a *unrest) getBackupRestoreTarget(_ context.Context) (RestoreTarget, error) {
	switch a.cfg.Restic.RestoreTarget {
	case targetFilesystem:
		return &FilesystemTarget{
			target: a.cfg.Filesystem,
		}, nil
	case targetSqlite:
		return &SqliteTarget{
			target: a.cfg.Sqlite,
		}, nil
	case targetMariaDb:
		return &MariaDBTarget{
			connectionInfo: a.cfg.Mariadb,
		}, nil
	case targetPostgres:
		return &PostgreSQLTarget{
			connectionInfo: a.cfg.Postgres,
		}, nil
	default:
		return nil, fmt.Errorf("unknown target type: %s", a.cfg.Restic.RestoreTarget)
	}
}

type RestoreTarget interface {
	restore(ctx context.Context, snapshot ResticSnapshot, resticOpts ResticRestoreRequest) error
}

type FilesystemTarget struct {
	target FilesystemParams
}

func (t *FilesystemTarget) restore(ctx context.Context, snapshot ResticSnapshot, resticOpts ResticRestoreRequest) error {
	snapshotId := snapshot.ID
	if resticOpts.SnapshotRestoreSubfolder != resticRestoreSubpathDefault && resticOpts.SnapshotRestoreSubfolder != "" {
		snapshotId = fmt.Sprintf("%s:%s", snapshot.ID, resticOpts.SnapshotRestoreSubfolder)
	}

	//nolint G204
	cmd := exec.CommandContext(ctx,
		"restic",
		"restore",
		fmt.Sprintf("--dry-run=%t", resticOpts.RestoreDryRun),
		fmt.Sprintf("--delete=%t", resticOpts.RestoreDelete),
		fmt.Sprintf("--target=%s", t.target.RestorePath),
		snapshotId)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	fmt.Println(cmd)

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("restore failed: %w", err)
	}

	return nil
}

type SqliteTarget struct {
	target SqliteConnection
}

func (t *SqliteTarget) restore(ctx context.Context, snapshot ResticSnapshot, resticOpts ResticRestoreRequest) error {
	//nolint G204
	var sqliteCmd = exec.Command("sqlite", t.target.DatabasePath)
	return pipeCommand(ctx, snapshot, resticOpts, sqliteCmd)
}

type MariaDBTarget struct {
	connectionInfo MariadbConnection
}

func (t *MariaDBTarget) restore(ctx context.Context, snapshot ResticSnapshot, resticOpts ResticRestoreRequest) error {
	var mysqlCmd *exec.Cmd
	var args []string

	if t.connectionInfo.ContainerName != "" {
		// Docker/Podman exec
		args = []string{
			"exec", "-i", t.connectionInfo.ContainerName,
			"mysql",
			fmt.Sprintf("--user=%s", t.connectionInfo.User),
			fmt.Sprintf("--password=%s", t.connectionInfo.Pass),
		}
		if t.connectionInfo.DatabaseName != "" {
			args = append(args, fmt.Sprintf("--database=%s", t.connectionInfo.DatabaseName))
		}
		mysqlCmd = exec.Command("docker", args...)
	} else {
		// Direct MySQL command
		args = []string{
			fmt.Sprintf("--host=%s", t.connectionInfo.Host),
			fmt.Sprintf("--port=%s", t.connectionInfo.Port),
			fmt.Sprintf("--user=%s", t.connectionInfo.User),
			fmt.Sprintf("--password=%s", t.connectionInfo.Pass),
		}
		if t.connectionInfo.DatabaseName != "" {
			args = append(args, fmt.Sprintf("--database=%s", t.connectionInfo.DatabaseName))
		}
		mysqlCmd = exec.Command("mysql", args...)
	}

	return pipeCommand(ctx, snapshot, resticOpts, mysqlCmd)
}

type PostgreSQLTarget struct {
	connectionInfo PostgresConnection
}

func (t *PostgreSQLTarget) restore(ctx context.Context, snapshot ResticSnapshot, resticOpts ResticRestoreRequest) error {
	var psqlCmd *exec.Cmd
	var args []string

	if t.connectionInfo.ContainerName != "" {
		// Docker/Podman exec
		args = []string{
			"exec", "-i", t.connectionInfo.ContainerName,
			"psql",
			fmt.Sprintf("--username=%s", t.connectionInfo.User),
		}
		if t.connectionInfo.DatabaseName != "" {
			args = append(args, fmt.Sprintf("--dbname=%s", t.connectionInfo.DatabaseName))
		}

		psqlCmd = exec.Command("docker", args...)

		// Set password as environment variable only if defined
		if t.connectionInfo.Pass != "" {
			psqlCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", t.connectionInfo.Pass))
		}
	} else {
		// Direct psql connection
		args = []string{
			fmt.Sprintf("--host=%s", t.connectionInfo.Host),
			fmt.Sprintf("--port=%s", t.connectionInfo.Port),
			fmt.Sprintf("--username=%s", t.connectionInfo.User),
		}
		if t.connectionInfo.DatabaseName != "" {
			args = append(args, fmt.Sprintf("--dbname=%s", t.connectionInfo.DatabaseName))
		}

		psqlCmd = exec.Command("psql", args...)

		if t.connectionInfo.Pass != "" {
			psqlCmd.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", t.connectionInfo.Pass))
		}
	}

	return pipeCommand(ctx, snapshot, resticOpts, psqlCmd)
}

func pipeCommand(ctx context.Context, snapshot ResticSnapshot, resticOpts ResticRestoreRequest, restoreCommand *exec.Cmd) error {
	//nolint G204
	resticCmd := exec.CommandContext(ctx, "restic", "dump", snapshot.ID, resticOpts.SnapshotDumpFile)

	// Pipe restic output to psql input
	pipe, err := resticCmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create pipe: %w", err)
	}

	restoreCommand.Stdin = pipe
	restoreCommand.Stdout = os.Stdout
	restoreCommand.Stderr = os.Stderr

	// Start restorecommand first
	if err := restoreCommand.Start(); err != nil {
		return fmt.Errorf("failed to start restore command: %w", err)
	}

	// Start restic
	if err := resticCmd.Start(); err != nil {
		return fmt.Errorf("failed to start restic: %w", err)
	}

	// Wait for both processes
	if err := resticCmd.Wait(); err != nil {
		return fmt.Errorf("restic dump failed: %w", err)
	}

	if err := restoreCommand.Wait(); err != nil {
		return fmt.Errorf("restore command failed: %w", err)
	}

	return nil
}

var validateFuncNotEmpty = func(s string) error {
	if strings.TrimSpace(s) == "" {
		return fmt.Errorf("value cannot be empty")
	}
	return nil
}

var validateFuncPort = func(s string) error {
	if strings.TrimSpace(s) == "" {
		return fmt.Errorf("port cannot be empty")
	}

	val, err := strconv.Atoi(s)
	if err != nil {
		return fmt.Errorf("not a number")
	}

	if val < 1 || val > 65535 {
		return fmt.Errorf("invalid port number")
	}

	return nil
}

func mustGetEnv(defaultVal string, keys ...string) string {
	val, err := getEnv(defaultVal, keys...)
	if err != nil {
		if errors.Is(err, ErrMultipleEnvVarsFound) {
			log.Fatal(err)
		}

		log.Fatalf("failed to get environment variable: %s", err)
	}

	return val
}

func getEnv(defaultVal string, keys ...string) (string, error) {
	type keyValPairs struct {
		key, value string
	}

	var keysFound []keyValPairs
	for _, envKey := range keys {
		if val := os.Getenv(envKey); val != "" {
			if strings.HasSuffix(strings.ToLower(envKey), "_file") {
				data, err := os.ReadFile(val)
				if err != nil {
					return "", fmt.Errorf("could not read file %q for %q: %w", val, envKey, err)
				}
				keysFound = append(keysFound, keyValPairs{envKey, strings.TrimSpace(string(data))})
			} else {
				keysFound = append(keysFound, keyValPairs{envKey, val})
			}
		}
	}

	switch len(keysFound) {
	case 0:
		return defaultVal, nil
	case 1:
		return keysFound[0].value, nil
	default:
		affectedEnvVars := make([]string, 0, len(keysFound))
		for _, key := range keysFound {
			affectedEnvVars = append(affectedEnvVars, key.key)
		}
		return keysFound[0].value, fmt.Errorf("%w: %s", ErrMultipleEnvVarsFound, strings.Join(affectedEnvVars, ","))
	}
}

func getBoolEnv(key string, defaultVal bool) bool {
	val := os.Getenv(key)
	if val == "" {
		return defaultVal
	}
	return val == "1" || strings.ToLower(val) == "true"
}

func (a *unrest) promptForMissingData(ctx context.Context, restic *ResticRestoreRequest) error {
	var groups []*huh.Group
	// Group 0: ask for restic repo and password
	groups = append(groups, huh.NewGroup(
		huh.NewInput().
			Title("Restic Repository (e.g. s3:https://s3.amazonaws.com/restic/keys)").
			Value(&restic.Repository).
			Validate(validateFuncNotEmpty),
		huh.NewInput().
			Title("Restic Password").
			EchoMode(huh.EchoModePassword).
			Value(&restic.Password).
			Validate(validateFuncNotEmpty),
	).WithHideFunc(func() bool {
		return restic.Repository != "" && restic.Password != ""
	},
	))

	// Group 1: Snapshot selection
	if restic.SnapshotId == "" {
		snapshotList := make([]ResticSnapshot, 0, len(a.resticSnapshots))
		for id, snap := range a.resticSnapshots {
			if id != "latest" {
				snapshotList = append(snapshotList, snap)
			}
		}

		slices.SortFunc(snapshotList, func(a, b ResticSnapshot) int {
			return b.Time.Compare(a.Time)
		})

		options := make([]huh.Option[string], len(snapshotList))
		for idx, snap := range snapshotList {
			label := fmt.Sprintf("%s | %s | %s | %s | %s",
				snap.Time.Format("2006-01-02 15:04:05"),
				snap.ShortID,
				snap.Hostname,
				strings.Join(snap.Tags, ", "),
				strings.Join(snap.Paths, ", "))
			options[idx] = huh.NewOption(label, snap.ID)
		}

		groups = append(groups, huh.NewGroup(
			huh.NewSelect[string]().
				Title("Select snapshot to restore").
				Options(options...).
				Value(&restic.SnapshotId),
		))
	}

	// Group 2: Restore target selection
	if restic.RestoreTarget == "" {
		groups = append(groups, huh.NewGroup(
			huh.NewSelect[string]().
				Title("Select restore target type").
				Options(
					huh.NewOption("Filesystem (restore)", targetFilesystem),
					huh.NewOption("Sqlite (streaming dump)", targetSqlite),
					huh.NewOption("MariaDB (streaming dump)", targetMariaDb),
					huh.NewOption("PostgreSQL (streaming dump)", targetPostgres),
				).
				Value(&restic.RestoreTarget),
		))
	}

	// Group 3: Snapshot path selection (conditional on target type)

	snapshot := a.resticSnapshots[restic.SnapshotId]
	if restic.SnapshotDumpFile == "" {
		groups = append(groups, huh.NewGroup(
			huh.NewSelect[string]().
				Value(&restic.SnapshotDumpFile).
				Height(1+len(snapshot.Paths)).
				Title("Select file in snapshot to stream to database").
				OptionsFunc(func() []huh.Option[string] {
					return huh.NewOptions(snapshot.Paths...)
				}, &snapshot),
		).WithHideFunc(
			func() bool {
				return restic.RestoreTarget == targetFilesystem || len(snapshot.Paths) < 2
			},
		))
	}

	if restic.SnapshotRestoreSubfolder == "" {
		groups = append(groups, huh.NewGroup(
			huh.NewSelect[string]().
				Value(&restic.SnapshotRestoreSubfolder).
				Height(2+len(snapshot.Paths)).
				Title("Select subfolder to restore").
				OptionsFunc(func() []huh.Option[string] {
					opts := []string{resticRestoreSubpathDefault}
					return huh.NewOptions(append(opts, snapshot.Paths...)...)
				}, &snapshot),
		).WithHideFunc(
			func() bool {
				return restic.RestoreTarget != targetFilesystem || len(snapshot.Paths) < 2
			},
		))
	}

	// Group 4: Restore path input
	var resticRestorePath string
	groups = append(groups, huh.NewGroup(
		huh.NewInput().
			TitleFunc(func() string {
				if restic.RestoreTarget == targetSqlite {
					return "Sqlite database path"
				}
				return "Choose the directory where to restore dump to"
			}, &restic).
			Value(&resticRestorePath).
			Validate(validateFuncNotEmpty),
	).WithHideFunc(func() bool {
		return restic.RestoreTarget != targetFilesystem && restic.RestoreTarget != targetSqlite
	}))

	// Group 5: Filesystem restore options
	var (
		optDryRun            = "dry-run"
		optDelete            = "delete"
		resticRestoreOptions []string
	)
	if a.cfg.Restic.RestoreDryRun {
		resticRestoreOptions = append(resticRestoreOptions, optDryRun)
	}
	if a.cfg.Restic.RestoreDelete {
		resticRestoreOptions = append(resticRestoreOptions, optDelete)
	}

	groups = append(groups,
		huh.NewGroup(
			huh.NewMultiSelect[string]().
				Title("Restore Options").
				Description("Select options to enable").
				Options(
					huh.NewOption("Dry Run (preview changes without changing anything on disk)", optDryRun),
					huh.NewOption("Delete (remove files in target folder that are not existent in snapshot)", optDelete),
				).
				Value(&resticRestoreOptions),
		).WithHideFunc(func() bool {
			return restic.RestoreTarget != targetFilesystem
		},
		))

	// Group 6: Database configuration (PostgreSQL)
	groups = append(groups, huh.NewGroup(
		huh.NewInput().
			Title("Database name").
			Description("Leave empty if dump includes multiple databases").
			Value(&a.cfg.Postgres.DatabaseName),
		huh.NewInput().
			Title("Database user").
			Validate(validateFuncNotEmpty).
			Value(&a.cfg.Postgres.User),
		huh.NewInput().
			Title("Database password").
			EchoMode(huh.EchoModePassword).
			Validate(validateFuncNotEmpty).
			Value(&a.cfg.Postgres.Pass),
		huh.NewInput().
			Title("Database host").
			Validate(validateFuncNotEmpty).
			Value(&a.cfg.Postgres.Host),
		huh.NewInput().
			Title("Database port").
			Validate(validateFuncPort).
			Value(&a.cfg.Postgres.Port),
		huh.NewInput().
			Title("Container name (optional, for Docker/Podman)").
			Description("Leave empty to use direct connection").
			Value(&a.cfg.Postgres.ContainerName),
	).WithHideFunc(
		func() bool {
			return restic.RestoreTarget != targetPostgres
		},
	))

	// Group 7: Database configuration (MariaDB)
	groups = append(groups, huh.NewGroup(
		huh.NewInput().
			Title("Database name").
			Description("Leave empty if dump includes multiple databases").
			Value(&a.cfg.Mariadb.DatabaseName),
		huh.NewInput().
			Title("Database user").
			Validate(validateFuncNotEmpty).
			Value(&a.cfg.Mariadb.User),
		huh.NewInput().
			Title("Database password").
			EchoMode(huh.EchoModePassword).
			Validate(validateFuncNotEmpty).
			Value(&a.cfg.Mariadb.Pass),
		huh.NewInput().
			Title("Database host").
			Validate(validateFuncNotEmpty).
			Value(&a.cfg.Mariadb.Host),
		huh.NewInput().
			Title("Database port").
			Validate(validateFuncPort).
			Value(&a.cfg.Mariadb.Port),
		huh.NewInput().
			Title("Container name (optional, for Docker/Podman)").
			Description("Leave empty to use direct connection").
			Value(&a.cfg.Mariadb.ContainerName),
	).WithHideFunc(
		func() bool {
			return restic.RestoreTarget != targetMariaDb
		},
	))

	form := huh.NewForm(groups...)
	if err := form.WithTimeout(a.cfg.AppOptions.SelectionTimeout).RunWithContext(ctx); err != nil {
		return fmt.Errorf("form error: %w", err)
	}

	// Update fields changed by huh
	a.cfg.Restic.RestoreDryRun = false
	a.cfg.Restic.RestoreDelete = false

	for _, opt := range resticRestoreOptions {
		switch opt {
		case optDryRun:
			a.cfg.Restic.RestoreDryRun = true
		case optDelete:
			a.cfg.Restic.RestoreDelete = true
		}
	}

	// update restore path / sqlite database path
	switch a.cfg.Restic.RestoreTarget {
	case targetSqlite:
		a.cfg.Sqlite.DatabasePath = resticRestorePath
	case targetFilesystem:
		a.cfg.Filesystem.RestorePath = resticRestorePath
	}

	// Display final dialog confirmation
	var confirm bool
	form = huh.NewForm(
		huh.NewGroup(
			huh.NewConfirm().
				Title("Perform restore?").
				DescriptionFunc(func() string {
					conf, err := a.getRestoreTargetConfig()
					if err != nil {
						return "unknown"
					}

					return fmt.Sprintf("\n%s\n%s", restic, conf)
				}, &restic).
				Affirmative("Yes").
				Negative("No").
				Value(&confirm),
		))

	if err := form.WithTimeout(a.cfg.AppOptions.SelectionTimeout).Run(); err != nil {
		return fmt.Errorf("form error: %w", err)
	}

	if !confirm {
		return errors.New("restore cancelled by user")
	}

	return nil
}
