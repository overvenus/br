package cmd

import (
	"github.com/pingcap/errors"
	"github.com/spf13/cobra"
)

// NewFullBackupCommand return a full backup subcommand.
func NewFullBackupCommand() *cobra.Command {
	raw := &cobra.Command{
		Use:   "full [flags]",
		Short: "backup the whole TiKV cluster",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := GetDefaultRawClient()
			return client.FullBackup()
		},
	}
	return raw
}

// NewRegionCommand return a backup region subcommand.
func NewRegionCommand() *cobra.Command {
	command := &cobra.Command{
		Use:   "region [flags]",
		Short: "backup specified regions",
		RunE: func(cmd *cobra.Command, _ []string) error {
			client := GetDefaultRawClient()
			regionID, err := cmd.Flags().GetUint64("region")
			if err != nil {
				return err
			}
			backer := GetDefaultBacker()
			retryCount := 5
			retryErr := errors.Errorf("exceed max retry %d", retryCount)
			for i := 0; i < retryCount; i++ {
				region, _, err := backer.GetPDClient().GetRegionByID(backer.Context(), regionID)
				if err != nil {
					return err
				}
				needRrtry, err := client.BackupRegion(region)
				if err != nil {
					return err
				}
				if !needRrtry {
					retryErr = nil
					break
				}
			}
			return retryErr
		},
	}
	command.Flags().Uint64P("region", "r", 0, "backup the specific regions")
	command.MarkFlagRequired("region")
	return command
}
