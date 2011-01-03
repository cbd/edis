%%%-------------------------------------------------------------------
%%% @doc Macros used to selectively send debugging information
%%%-------------------------------------------------------------------

%%NOTE: We do this because we want to step over other definitions of these macros
-undef(LOG_LEVEL_DEBUG).
-undef(LOG_LEVEL_INFO).
-undef(LOG_LEVEL_STAT).
-undef(LOG_LEVEL_WARN).
-undef(LOG_LEVEL_ERROR).
-undef(LOG_LEVEL_FATAL).
-undef(LOG_LEVELS).
-undef(LOG).
-undef(DEBUG).
-undef(INFO).
-undef(WARN).
-undef(STAT).
-undef(ERROR).
-undef(FATAL).

-define(LOG_LEVEL_DEBUG, debug).
-define(LOG_LEVEL_INFO,  info).
-define(LOG_LEVEL_STAT,  stat).
-define(LOG_LEVEL_WARN,  warn).
-define(LOG_LEVEL_ERROR, error).
-define(LOG_LEVEL_FATAL, fatal).
-define(LOG_LEVELS, [?LOG_LEVEL_DEBUG, ?LOG_LEVEL_INFO, ?LOG_LEVEL_STAT,
                     ?LOG_LEVEL_WARN, ?LOG_LEVEL_ERROR, ?LOG_LEVEL_FATAL]).
-type loglevel() :: ?LOG_LEVEL_DEBUG | ?LOG_LEVEL_INFO | ?LOG_LEVEL_STAT | ?LOG_LEVEL_WARN | ?LOG_LEVEL_ERROR | ?LOG_LEVEL_FATAL.

-record(log, {time = erlang:localtime() :: {{integer(),integer(),integer()},{integer(),integer(),integer()}},
              level                     :: loglevel(),
              module                    :: atom(),
              line                      :: integer(),
              pid                       :: pid(),
              node                      :: node(),
              stacktrace = []           :: [term()],
              text = ""                 :: string(),
              args = []                 :: [term()]}).

-define(LOG(LOGProcess, LOGLevel, LOGStr, LOGArgs, LOGStack),
        try
          gen_server:cast(LOGProcess,
                          #log{module      = ?MODULE,
                               level       = LOGLevel,
                               line        = ?LINE,
                               pid         = self(),
                               node        = node(),
                               stacktrace  = LOGStack,
                               text        = LOGStr,
                               args        = LOGArgs})
        catch
            _ ->
                error_logger:error_msg("Exception trying to log a message:~p~n",
                                       [{LOGStr, LOGArgs}])
        end).
-define(DEBUG(Str, Args), io:format(Str,Args)).
-define(INFO(Str, Args),  ok = hd([ok, Str | Args])).
-define(STAT(Str, Args),  ?LOG('edis-stat', ?LOG_LEVEL_STAT, Str, Args, [])).
-define(WARN(Str, Args),  ?LOG('edis-warn', ?LOG_LEVEL_WARN, Str, Args, [])).
-define(ERROR(Str, Args), ?LOG('edis-error', ?LOG_LEVEL_ERROR, Str, Args, erlang:get_stacktrace())).
-define(THROW(Str, Args), try throw({}) catch _:_ -> ?LOG('edis-error', ?LOG_LEVEL_ERROR, Str, Args, erlang:get_stacktrace()) end).
-define(FATAL(Str, Args), ?LOG('edis-fatal', ?LOG_LEVEL_FATAL, Str, Args, erlang:get_stacktrace())).
