-include("elog.hrl").

-record(edis_command, {timestamp    :: float(),
                       db           :: non_neg_integer(),
                       cmd          :: atom(),
                       args = []    :: [term()]}).