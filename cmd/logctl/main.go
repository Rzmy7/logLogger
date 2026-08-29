package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/Rzmy7/logLogger/cmd/logctl/client"
)

// App encapsulates dependencies and I/O streams for logctl.
type App struct {
	client *client.Client
	stdout io.Writer
	stderr io.Writer
	stdin  io.Reader
}

func main() {
	app := &App{
		stdout: os.Stdout,
		stderr: os.Stderr,
		stdin:  os.Stdin,
	}

	exitCode := app.Run(os.Args[1:])
	os.Exit(exitCode)
}

func normalizeArgs(args []string, valueFlags ...string) []string {
	valMap := make(map[string]bool)
	for _, f := range valueFlags {
		valMap[f] = true
	}

	var flags []string
	var positional []string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if strings.HasPrefix(arg, "-") {
			name := strings.TrimLeft(arg, "-")
			if strings.Contains(name, "=") {
				flags = append(flags, arg)
				continue
			}
			flags = append(flags, arg)
			if valMap[name] && i+1 < len(args) && !strings.HasPrefix(args[i+1], "-") {
				flags = append(flags, args[i+1])
				i++
			}
		} else {
			positional = append(positional, arg)
		}
	}
	return append(flags, positional...)
}

func (a *App) printUsage() {
	usage := `logctl - Administrative CLI for the Log Platform

Usage:
  logctl [global flags] <command> [subcommand] [flags]

Commands:
  health                     Check health and status of the Analytics API and dependencies
  logs stats                 Show storage, document counts, and index statistics
  logs search [flags]        Search and filter indexed log documents
  logs delete-index <name>   Delete a specific daily log index (destructive)
  logs delete-before <time>  Delete all indices older than an RFC3339 timestamp (destructive)
  retention status           Display retention lifecycle summary and index stats
  retention run [flags]      Trigger manual index retention cleanup

Global Flags:
  --api-url string           Analytics API base URL (default $LOGCTL_API_URL or http://localhost:8082)
  --json                     Output raw machine-readable JSON to stdout
  --help, -h                 Show help for logctl

Use "logctl <command> --help" for more information about a command.
`
	fmt.Fprint(a.stdout, usage)
}

// Run executes the CLI with the provided arguments and returns an exit code.
func (a *App) Run(args []string) int {
	if len(args) == 0 {
		a.printUsage()
		return 1
	}

	// Parse global pre-flags
	var globalAPIURL string
	var globalJSON bool
	var remainingArgs []string

	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "-h" || arg == "--help" || arg == "help" {
			a.printUsage()
			return 0
		}
		if arg == "--json" {
			globalJSON = true
			continue
		}
		if arg == "--api-url" && i+1 < len(args) {
			globalAPIURL = args[i+1]
			i++
			continue
		}
		if strings.HasPrefix(arg, "--api-url=") {
			globalAPIURL = strings.TrimPrefix(arg, "--api-url=")
			continue
		}
		remainingArgs = append(remainingArgs, arg)
	}

	if len(remainingArgs) == 0 {
		a.printUsage()
		return 1
	}

	opts := []client.Option{}
	if globalAPIURL != "" {
		opts = append(opts, client.WithBaseURL(globalAPIURL))
	}
	a.client = client.NewClient(opts...)

	cmd := remainingArgs[0]
	cmdArgs := remainingArgs[1:]

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch cmd {
	case "health":
		return a.runHealth(ctx, cmdArgs, globalJSON)
	case "logs":
		return a.runLogs(ctx, cmdArgs, globalJSON)
	case "retention":
		return a.runRetention(ctx, cmdArgs, globalJSON)
	default:
		fmt.Fprintf(a.stderr, "Error: unknown command %q\n\n", cmd)
		a.printUsage()
		return 1
	}
}

// ==========================================
// Command: health
// ==========================================

func (a *App) runHealth(ctx context.Context, args []string, globalJSON bool) int {
	args = normalizeArgs(args)
	fs := flag.NewFlagSet("health", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	jsonFlag := fs.Bool("json", globalJSON, "Output machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	resp, err := a.client.Health(ctx)
	if err != nil {
		fmt.Fprintf(a.stderr, "Error: %v\n", err)
		return 1
	}

	if *jsonFlag || globalJSON {
		return a.printJSON(resp)
	}

	w := tabwriter.NewWriter(a.stdout, 0, 0, 3, ' ', 0)
	fmt.Fprintf(w, "STATUS\t%s\n", resp.Status)
	for dep, st := range resp.Dependencies {
		fmt.Fprintf(w, "DEPENDENCY (%s)\t%s\n", dep, st)
	}
	fmt.Fprintf(w, "TIMESTAMP\t%s\n", resp.Meta.Timestamp)
	fmt.Fprintf(w, "REQUEST ID\t%s\n", resp.Meta.RequestID)
	w.Flush()
	return 0
}

// ==========================================
// Command: logs
// ==========================================

func (a *App) runLogs(ctx context.Context, args []string, globalJSON bool) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprint(a.stdout, `Usage: logctl logs <subcommand> [flags]

Subcommands:
  stats                      Show storage, document counts, and index statistics
  search [flags]             Search and filter indexed log documents
  delete-index <name> [flags] Delete a specific daily log index (destructive)
  delete-before <time> [flags] Delete all indices older than RFC3339 timestamp (destructive)
`)
		return 0
	}

	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "stats":
		return a.runLogsStats(ctx, subArgs, globalJSON)
	case "search":
		return a.runLogsSearch(ctx, subArgs, globalJSON)
	case "delete-index":
		return a.runLogsDeleteIndex(ctx, subArgs, globalJSON)
	case "delete-before":
		return a.runLogsDeleteBefore(ctx, subArgs, globalJSON)
	default:
		fmt.Fprintf(a.stderr, "Error: unknown logs subcommand %q\n", sub)
		return 1
	}
}

func (a *App) runLogsStats(ctx context.Context, args []string, globalJSON bool) int {
	args = normalizeArgs(args)
	fs := flag.NewFlagSet("logs stats", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	jsonFlag := fs.Bool("json", globalJSON, "Output machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	resp, err := a.client.GetLogStats(ctx)
	if err != nil {
		fmt.Fprintf(a.stderr, "Error: %v\n", err)
		return 1
	}

	if *jsonFlag || globalJSON {
		return a.printJSON(resp)
	}

	fmt.Fprintf(a.stdout, "Storage Statistics\n")
	fmt.Fprintf(a.stdout, "------------------\n")
	fmt.Fprintf(a.stdout, "Total Logs:     %d\n", resp.Data.TotalLogs)
	fmt.Fprintf(a.stdout, "Total Indices:  %d\n", resp.Data.TotalIndices)
	fmt.Fprintf(a.stdout, "Total Size:     %s (%.2f MB)\n", formatBytes(resp.Data.TotalSizeBytes), float64(resp.Data.TotalSizeBytes)/(1024*1024))
	fmt.Fprintf(a.stdout, "Oldest Index:   %s (%s)\n", resp.Data.OldestIndex, resp.Data.OldestLogDate)
	fmt.Fprintf(a.stdout, "Newest Index:   %s (%s)\n\n", resp.Data.NewestIndex, resp.Data.NewestLogDate)

	if len(resp.Data.Indices) > 0 {
		w := tabwriter.NewWriter(a.stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "INDEX NAME\tDOCUMENTS\tSIZE\tSTATUS\tCREATED")
		for _, idx := range resp.Data.Indices {
			fmt.Fprintf(w, "%s\t%d\t%s\t%s\t%s\n", idx.Name, idx.DocCount, formatBytes(idx.StoreSizeBytes), idx.Status, idx.CreationDate)
		}
		w.Flush()
	}
	return 0
}

func (a *App) runLogsSearch(ctx context.Context, args []string, globalJSON bool) int {
	args = normalizeArgs(args, "service", "level", "query", "q", "trace-id", "tenant-id", "from", "to", "page", "size")
	fs := flag.NewFlagSet("logs search", flag.ContinueOnError)
	fs.SetOutput(a.stderr)

	service := fs.String("service", "", "Filter by service name")
	level := fs.String("level", "", "Filter by log level (DEBUG, INFO, WARN, ERROR, FATAL)")
	query := fs.String("query", "", "Full-text query string in log message")
	fs.StringVar(query, "q", "", "Alias for --query")
	traceID := fs.String("trace-id", "", "Filter by trace_id")
	tenantID := fs.String("tenant-id", "", "Filter by tenant_id")
	from := fs.String("from", "", "Filter from timestamp (RFC3339)")
	to := fs.String("to", "", "Filter to timestamp (RFC3339)")
	page := fs.Int("page", 1, "Page number")
	size := fs.Int("size", 20, "Page size (max 100)")
	jsonFlag := fs.Bool("json", globalJSON, "Output machine-readable JSON")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	params := client.SearchParams{
		Query:    *query,
		Service:  *service,
		Level:    *level,
		TraceID:  *traceID,
		TenantID: *tenantID,
		From:     *from,
		To:       *to,
		Page:     *page,
		Size:     *size,
	}

	resp, err := a.client.SearchLogs(ctx, params)
	if err != nil {
		fmt.Fprintf(a.stderr, "Error: %v\n", err)
		return 1
	}

	if *jsonFlag || globalJSON {
		return a.printJSON(resp)
	}

	fmt.Fprintf(a.stdout, "Search Results (Total: %d, Page %d/%d)\n", resp.Data.Total, resp.Data.Page, (resp.Data.Total+int64(resp.Data.Size)-1)/int64(resp.Data.Size))
	if len(resp.Data.Logs) == 0 {
		fmt.Fprintln(a.stdout, "No matching logs found.")
		return 0
	}

	w := tabwriter.NewWriter(a.stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "TIMESTAMP\tLEVEL\tSERVICE\tTENANT\tTRACE ID\tMESSAGE")
	for _, doc := range resp.Data.Logs {
		tenant := doc.TenantID
		if tenant == "" {
			tenant = "default"
		}
		msg := doc.Message
		if len(msg) > 60 {
			msg = msg[:57] + "..."
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", doc.Timestamp, doc.Level, doc.Service, tenant, doc.TraceID, msg)
	}
	w.Flush()
	return 0
}

func (a *App) runLogsDeleteIndex(ctx context.Context, args []string, globalJSON bool) int {
	args = normalizeArgs(args)
	fs := flag.NewFlagSet("logs delete-index", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	yesFlag := fs.Bool("yes", false, "Skip interactive confirmation prompt")
	fs.BoolVar(yesFlag, "y", false, "Alias for --yes")
	jsonFlag := fs.Bool("json", globalJSON, "Output machine-readable JSON")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	remaining := fs.Args()
	if len(remaining) == 0 {
		fmt.Fprintln(a.stderr, "Error: missing required argument <index_name>")
		fmt.Fprintln(a.stderr, "Usage: logctl logs delete-index <index_name> [--yes] [--json]")
		return 1
	}
	indexName := remaining[0]

	if !*yesFlag {
		if !a.confirmPrompt(fmt.Sprintf("WARNING: Are you sure you want to permanently delete index %q?", indexName)) {
			fmt.Fprintln(a.stderr, "Operation canceled by user.")
			return 1
		}
	}

	resp, err := a.client.DeleteIndex(ctx, indexName)
	if err != nil {
		fmt.Fprintf(a.stderr, "Error: %v\n", err)
		return 1
	}

	if *jsonFlag || globalJSON {
		return a.printJSON(resp)
	}

	fmt.Fprintf(a.stdout, "Success: Index %q was deleted.\n", indexName)
	return 0
}

func (a *App) runLogsDeleteBefore(ctx context.Context, args []string, globalJSON bool) int {
	args = normalizeArgs(args)
	fs := flag.NewFlagSet("logs delete-before", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	yesFlag := fs.Bool("yes", false, "Skip interactive confirmation prompt")
	fs.BoolVar(yesFlag, "y", false, "Alias for --yes")
	jsonFlag := fs.Bool("json", globalJSON, "Output machine-readable JSON")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	remaining := fs.Args()
	if len(remaining) == 0 {
		fmt.Fprintln(a.stderr, "Error: missing required argument <RFC3339_timestamp>")
		fmt.Fprintln(a.stderr, "Usage: logctl logs delete-before <timestamp> [--yes] [--json]")
		return 1
	}
	beforeTimestamp := remaining[0]

	if !*yesFlag {
		if !a.confirmPrompt(fmt.Sprintf("WARNING: Are you sure you want to permanently delete all indices older than %q?", beforeTimestamp)) {
			fmt.Fprintln(a.stderr, "Operation canceled by user.")
			return 1
		}
	}

	resp, err := a.client.DeleteBefore(ctx, beforeTimestamp)
	if err != nil {
		fmt.Fprintf(a.stderr, "Error: %v\n", err)
		return 1
	}

	if *jsonFlag || globalJSON {
		return a.printJSON(resp)
	}

	fmt.Fprintf(a.stdout, "Deletion Summary\n")
	fmt.Fprintf(a.stdout, "----------------\n")
	fmt.Fprintf(a.stdout, "Evaluated Indices: %d\n", resp.Data.EvaluatedCount)
	fmt.Fprintf(a.stdout, "Deleted Indices:   %d\n", resp.Data.DeletedCount)
	fmt.Fprintf(a.stdout, "Cutoff Date:       %s\n", resp.Data.CutoffDate)
	if len(resp.Data.DeletedIndices) > 0 {
		fmt.Fprintf(a.stdout, "Deleted List:      %s\n", strings.Join(resp.Data.DeletedIndices, ", "))
	}
	return 0
}

// ==========================================
// Command: retention
// ==========================================

func (a *App) runRetention(ctx context.Context, args []string, globalJSON bool) int {
	if len(args) == 0 || args[0] == "-h" || args[0] == "--help" || args[0] == "help" {
		fmt.Fprint(a.stdout, `Usage: logctl retention <subcommand> [flags]

Subcommands:
  status                     Display current index lifecycle and retention status
  run [--days N]             Trigger manual retention cycle cleanup
`)
		return 0
	}

	sub := args[0]
	subArgs := args[1:]

	switch sub {
	case "status":
		return a.runRetentionStatus(ctx, subArgs, globalJSON)
	case "run":
		return a.runRetentionRun(ctx, subArgs, globalJSON)
	default:
		fmt.Fprintf(a.stderr, "Error: unknown retention subcommand %q\n", sub)
		return 1
	}
}

func (a *App) runRetentionStatus(ctx context.Context, args []string, globalJSON bool) int {
	args = normalizeArgs(args)
	fs := flag.NewFlagSet("retention status", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	jsonFlag := fs.Bool("json", globalJSON, "Output machine-readable JSON")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	resp, err := a.client.GetLogStats(ctx)
	if err != nil {
		fmt.Fprintf(a.stderr, "Error: %v\n", err)
		return 1
	}

	if *jsonFlag || globalJSON {
		return a.printJSON(resp)
	}

	fmt.Fprintf(a.stdout, "Log Retention & Storage Status\n")
	fmt.Fprintf(a.stdout, "==============================\n")
	fmt.Fprintf(a.stdout, "Total Daily Indices:  %d\n", resp.Data.TotalIndices)
	fmt.Fprintf(a.stdout, "Total Log Documents:  %d\n", resp.Data.TotalLogs)
	fmt.Fprintf(a.stdout, "Total Storage Size:   %s\n", formatBytes(resp.Data.TotalSizeBytes))
	fmt.Fprintf(a.stdout, "Oldest Active Index:  %s (%s)\n", resp.Data.OldestIndex, resp.Data.OldestLogDate)
	fmt.Fprintf(a.stdout, "Newest Active Index:  %s (%s)\n\n", resp.Data.NewestIndex, resp.Data.NewestLogDate)

	if len(resp.Data.Indices) > 0 {
		w := tabwriter.NewWriter(a.stdout, 0, 0, 3, ' ', 0)
		fmt.Fprintln(w, "INDEX\tDOCS\tSIZE\tSTATUS")
		for _, idx := range resp.Data.Indices {
			fmt.Fprintf(w, "%s\t%d\t%s\t%s\n", idx.Name, idx.DocCount, formatBytes(idx.StoreSizeBytes), idx.Status)
		}
		w.Flush()
	}
	return 0
}

func (a *App) runRetentionRun(ctx context.Context, args []string, globalJSON bool) int {
	args = normalizeArgs(args, "days")
	fs := flag.NewFlagSet("retention run", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	days := fs.Int("days", 30, "Retention threshold in days")
	jsonFlag := fs.Bool("json", globalJSON, "Output machine-readable JSON")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	resp, err := a.client.RunRetention(ctx, *days)
	if err != nil {
		fmt.Fprintf(a.stderr, "Error: %v\n", err)
		return 1
	}

	if *jsonFlag || globalJSON {
		return a.printJSON(resp)
	}

	fmt.Fprintf(a.stdout, "Retention Execution Complete\n")
	fmt.Fprintf(a.stdout, "----------------------------\n")
	fmt.Fprintf(a.stdout, "Evaluated Indices: %d\n", resp.Data.EvaluatedCount)
	fmt.Fprintf(a.stdout, "Deleted Indices:   %d\n", resp.Data.DeletedCount)
	fmt.Fprintf(a.stdout, "Cutoff Date:       %s\n", resp.Data.CutoffDate)
	fmt.Fprintf(a.stdout, "Duration:          %s\n", resp.Data.Duration)
	if len(resp.Data.DeletedIndices) > 0 {
		fmt.Fprintf(a.stdout, "Deleted Indices:   %s\n", strings.Join(resp.Data.DeletedIndices, ", "))
	}
	return 0
}

// ==========================================
// Helpers
// ==========================================

func (a *App) confirmPrompt(prompt string) bool {
	fmt.Fprintf(a.stdout, "%s [y/N]: ", prompt)
	reader := bufio.NewReader(a.stdin)
	response, err := reader.ReadString('\n')
	if err != nil {
		return false
	}
	response = strings.TrimSpace(strings.ToLower(response))
	return response == "y" || response == "yes"
}

func (a *App) printJSON(v any) int {
	enc := json.NewEncoder(a.stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		fmt.Fprintf(a.stderr, "Error encoding JSON output: %v\n", err)
		return 1
	}
	return 0
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
