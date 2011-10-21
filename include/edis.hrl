-include("elog.hrl").

-record(edis_command, {timestamp = edis_util:timestamp()
                                    :: float(),
                       db           :: non_neg_integer(),
                       cmd          :: binary(),
                       args = []    :: [term()]}).

-record(edis_item, {key               :: binary(),
                    type              :: edis_db:item_type(),
                    encoding          :: edis_db:item_encoding(),
                    value             :: term(),
                    expire = infinity :: infinity | pos_integer()}).