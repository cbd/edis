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

-type config_option() :: listener_port_range | client_timeout | databases | requirepass | dir | backend.

%% @doc sets configuration params
-spec set(config_option(), term()) -> ok.
set(listener_port_range, {P1, P2}) when is_integer(P1), is_integer(P2), P1 =< P2 ->
  ok = application:set_env(edis, listener_port_range, {P1, P2}),
  edis_listener_sup:reload();
set(listener_port_range, Param) ->
  lager:alert("Invalid range: ~p~n", [Param]),
  throw(invalid_param);
set(client_timeout, Timeout) when is_integer(Timeout), Timeout >= 0 ->
  ok = application:set_env(edis, client_timeout, Timeout);
set(client_timeout, Param) ->
  lager:alert("Invalid timeout: ~p~n", [Param]),
  throw(invalid_param);
set(databases, Dbs) when is_integer(Dbs), Dbs > 0 ->
  ok = application:set_env(edis, databases, Dbs),
  edis_db_sup:reload();
set(databases, Param) ->
  lager:alert("Invalid number: ~p~n", [Param]),
  throw(invalid_param);
set(requirepass, Pass) ->
  ok = application:set_env(edis, requirepass, Pass);
set(dir, Dir) when is_binary(Dir) ->
  ok = application:set_env(edis, dir, binary_to_list(Dir)),
  edis_db_sup:reload();
set(dir, Param) ->
  lager:alert("Invalid dir: ~p~n", [Param]),
  throw(invalid_param);
set(Param, Value) ->
  lager:alert("Unsupported param: ~p: ~p~n", [Param, Value]),
  throw(unsupported_param).

%% @doc gets configuration params
-spec get(binary() | config_option()) -> term().
get(listener_port_range) ->
  get(listener_port_range, {6379,6379});
get(client_timeout) ->
  get(client_timeout, 35000);
get(databases) ->
  get(databases, 16);
get(requirepass) ->
  get(requirepass, undefined);
get(dir) ->
  get(dir, "./db/");
get(backend) ->
  get(backend, {edis_eleveldb_backend, [{create_if_missing, true}]});
get(Pattern) ->
  [{K, V} ||
   {K, V} <- application:get_all_env(edis),
   re:run(atom_to_binary(K, utf8), Pattern) =/= nomatch].

get(Field, Default) ->
  case application:get_env(edis, Field) of
    {ok, Value} ->
      lager:debug("~p := ~p~n", [Field, Value]),
      Value;
    _ ->
      lager:debug("~p := ~p~n", [Field, Default]),
      Default
  end.
