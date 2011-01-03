-module(config).
-compile([export_all]).

-include("log.hrl").

get_env(Field, Default) ->
  case application:get_env(italk, Field) of
    {ok, Value} ->
      ?DEBUG("~p := ~p~n", [Field, Value]),
      Value;
    _ ->
      ?DEBUG("~p := ~p~n", [Field, Default]),
      Default
  end.


