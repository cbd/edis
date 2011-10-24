-include("elog.hrl").

-type edis_sort_field() :: self | binary() | {binary(), binary()}.
-record(edis_sort_options, {by = self       :: edis_sort_field(),
                            limit           :: undefined | {integer(), integer()},
                            get = [self]    :: [edis_sort_field()],
                            direction = asc :: asc | desc,
                            type = float    :: alpha | float,
                            store_in = none :: none | binary()}).

-record(edis_command, {timestamp = edis_util:timestamp()
                                    :: float(),
                       db           :: non_neg_integer(),
                       cmd          :: binary(),
                       args = []    :: [term()],
                       group        :: keys | strings | hashes | lists | sets | zsets | pub_sub | transactions | connection | server,
                       result_type  :: edis:result_type(),
                       timeout      :: undefined | infinity | pos_integer(),
                       expire       :: undefined | never | pos_integer()}).

-record(edis_item, {key               :: binary(),
                    type              :: edis_db:item_type(),
                    encoding          :: edis_db:item_encoding(),
                    value             :: term(),
                    expire = infinity :: infinity | pos_integer()}).