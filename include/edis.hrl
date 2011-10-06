-include("elog.hrl").

-record(edis_command, {timestamp    :: float(),
                       db           :: non_neg_integer(),
                       cmd          :: atom(),
                       args = []    :: [term()]}).

-record(edis_item, {key               :: binary(),
                    value             :: term(),
                    type              :: string | hash | list | set | zset,
                    expire = infinity :: infinity | pos_integer()}).