%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc edis Configuration utilities
%%% @end
%%%-------------------------------------------------------------------
-module(edis_config).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-export([get/1, set/2]).

-include("edis.hrl").

-type config_option() :: listener_port_range | client_timeout | databases | requirepass.

-spec set(config_option(), term()) -> ok.
set(listener_port_range, {P1, P2}) when is_integer(P1), is_integer(P2), P1 =< P2 ->
  ok = application:set_env(edis, listener_port_range, {P1, P2}),
  edis_listener_sup:reload();
set(listener_port_range, Param) ->
  ?THROW("Invalid range: ~p~n", [Param]),
  throw(invalid_param);
set(client_timeout, Timeout) when is_integer(Timeout), Timeout >= 0 ->
  ok = application:set_env(edis, client_tiemout, Timeout);
set(client_timeout, Param) ->
  ?THROW("Invalid timeout: ~p~n", [Param]),
  throw(invalid_param);
set(databases, Dbs) when is_integer(Dbs), Dbs > 0 ->
  ok = application:set_env(edis, databases, Dbs),
  edis_db_sup:reload();
set(databases, Param) ->
  ?THROW("Invalid number: ~p~n", [Param]),
  throw(invalid_param);
set(requirepass, undefined) ->
  ok = application:set_env(edis, requirepass, undefined);
set(requirepass, Pass) when is_binary(Pass) ->
  ok = application:set_env(edis, requirepass, Pass),
  edis_client_sup:reload();
set(requirepass, Param) ->
  ?THROW("Invalid password: ~p~n", [Param]),
  throw(invalid_param);
set(Param, Value) ->
  ?THROW("Unsupported param: ~p: ~p~n", [Param, Value]),
  throw(unsupported_param).

-spec get(binary() | config_option()) -> term().
get(listener_port_range) ->
  get(listener_port_range, {6379,6379});
get(client_timeout) ->
  get(client_tiemout, 35000);
get(databases) ->
  get(databases, 16);
get(requirepass) ->
  get(requirepass, undefined);
get(Pattern) ->
  [{K, V} ||
   {K, V} <- application:get_all_env(edis),
   re:run(atom_to_binary(K, utf8), Pattern) =/= nomatch].

get(Field, Default) ->
  case application:get_env(edis, Field) of
    {ok, Value} ->
      ?DEBUG("~p := ~p~n", [Field, Value]),
      Value;
    _ ->
      ?DEBUG("~p := ~p~n", [Field, Default]),
      Default
  end.