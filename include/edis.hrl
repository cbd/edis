
-define(POS_INFINITY,1.7976931348623157e308).
-define(NEG_INFINITY,-1.7976931348623157e308).

-type edis_sort_field() :: self | binary() | {binary(), binary()}.
-record(edis_sort_options, {by = self         :: edis_sort_field(),
                            limit = undefined :: undefined | {integer(), integer()},
                            get = []          :: [edis_sort_field()],
                            direction = asc   :: asc | desc,
                            type = default    :: alpha | default,
                            store_in          :: undefined | binary()}).

-record(edis_message, {channel  :: binary(),
                       message  :: binary()}).

-record(edis_command, {timestamp = edis_util:timestamp()
                                    :: float(),
                       db = 0       :: non_neg_integer(),
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
                    vclock = edis_vclock:fresh()           :: edis_vclock:vclock(), %type defined in edis_vclock.erl
                    timestamp = edis_vclock:timestamp()     :: integer(),
                    expire = infinity :: infinity | pos_integer()}).