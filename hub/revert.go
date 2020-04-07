package hub

import (
	"fmt"

	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/hashicorp/go-multierror"

	"github.com/greenplum-db/gpupgrade/idl"
	"github.com/greenplum-db/gpupgrade/step"
)

func (s *Server) Revert(_ *idl.RevertRequest, stream idl.CliToHub_RevertServer) (err error) {
	st, err := step.Begin(s.StateDir, "revert", stream)
	if err != nil {
		return err
	}

	defer func() {
		if ferr := st.Finish(); ferr != nil {
			err = multierror.Append(err, ferr).ErrorOrNil()
		}

		if err != nil {
			gplog.Error(fmt.Sprintf("revert: %s", err))
		}
	}()

	// Delete primary and mirror data directories
	err = DeleteSegmentAndStandbyDirectories(s.agentConns, s.targetCluster)
	if err != nil {
		return err
	}
	// Stop agents
	// Delete master directory
	// Option 1:
	// Stop gRPC server on hub
	// Remove state directory, not logging any errors
	// Kill master hub process, not logging any errors
	// Option 2:
	// Remove state directory
	// Hand control back to CLI to stop hub from there

	return st.Err()
}
