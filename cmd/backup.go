package cmd

import (
	_ "github.com/overvenus/br/backup"
	"github.com/spf13/cobra"
)

// NewBackupCommand return a backup subcommand.
func NewBackupCommand() *cobra.Command {
	backup := &cobra.Command{
		Use:   "backup <subcommand>",
		Short: "backup a TiDB/TiKV cluster",
	}
	backup.AddCommand(NewBackupWholeCommand())
	// TODO: backup.AddCommand(NewBackupTableCommand())
	return backup
}

func NewBackupWholeCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "whole [--interval=3m] [--concurrency=10]",
		Short: "backup whole cluster to learner nodes",
		Run: func(cmd *cobra.Command, args []string) {
			if len(args) != 1 {
				cmd.Println(cmd.UsageString())
				return
			}

			backer := GetDefaultBacker()
			v, err := backer.Backup(
				cmd.Flags().Lookup("concurrency").Value.Int(),
				cmd.Flags().Lookup("interval").Value.Duration(),
			)
			if err != nil {
				fmt.Println(err)
				return
			}
		},
	})
	c.Flags().Duration("interval", backup.BackupDefaultInterval, "the policy to get region split key")
	c.Flags().Int("concurrency", 10, "the policy to get region split key")
	return c
}
