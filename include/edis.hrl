-include("elog.hrl").

-record(edis_command, {name                      :: binary(),
                       args = 0                  :: non_neg_integer(),
                       last_arg_is_safe = false  :: boolean()}).