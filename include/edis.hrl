-include("elog.hrl").

-record(edis_command, {timestamp    :: float(),
                       db           :: non_neg_integer(),
                       cmd          :: atom(),
                       args = []    :: [term()]}).

-record(edis_item, {key               :: binary(),
                    type              :: edis_db:item_type(),
                    encoding          :: edis_db:item_encoding(),
                    value             :: term(),
                    expire = infinity :: infinity | pos_integer()}).