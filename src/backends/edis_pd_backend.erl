%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Process Dictionary based backend. <b>DON'T DO THIS AT HOME, KIDS</b>
%%% @end
%%%-------------------------------------------------------------------
-module(edis_pd_backend).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_backend).

-include("edis.hrl").

-record(ref, {}).
-opaque ref() :: #ref{}.
-export_type([ref/0]).

-export([init/3, write/2, put/3, delete/2, fold/3, is_empty/1, destroy/1, status/1, get/2]).

%% ====================================================================
%% Behaviour functions
%% ====================================================================
-spec init(string(), non_neg_integer(), [any()]) -> {ok, ref()} | {error, term()}.
init(_Dir, _Index, _Options) ->
  lager:warn("USING PD BACKEND!! This should not be a production environment!~n", []),
  {ok, #ref{}}.

-spec write(ref(), edis_backend:write_actions()) -> ok | {error, term()}.
write(#ref{}, Actions) ->
  lists:foreach(
    fun({put, Key, Item}) -> erlang:put(Key, Item);
       ({delete, Key}) -> erlang:erase(Key);
       (clear) -> erlang:erase()
    end, Actions).

-spec put(ref(), binary(), #edis_item{}) -> ok | {error, term()}.
put(#ref{}, Key, Item) ->
  _ = erlang:put(Key, Item), ok.

-spec delete(ref(), binary()) -> ok | {error, term()}.
delete(#ref{}, Key) ->
  _ = erlang:erase(Key), ok.

-spec fold(ref(), edis_backend:fold_fun(), term()) -> term().
fold(#ref{}, Fun, InitValue) ->
  lists:foldl(
    fun({_Key, Item}, Acc) ->
      Fun(Item, Acc)
    end, InitValue, [{K, V} || {K, V} <- erlang:get(), not is_atom(K)]).

-spec is_empty(ref()) -> boolean().
is_empty(#ref{}) -> [] =:= erlang:get().

-spec destroy(ref()) -> ok | {error, term()}.
destroy(#ref{}) ->
  lists:foreach(fun({K,V}) when is_atom(K) -> erlang:put(K, V);
                   (_) -> ok
                end, erlang:erase()).

-spec status(ref()) -> {ok, binary()} | error.
status(#ref{}) -> {ok, iolist_to_binary(io_lib:format("~p~n", [erlang:process_info(self(), dictionary)]))}.

-spec get(ref(), binary()) -> #edis_item{} | not_found | {error, term()}.
get(#ref{}, Key) ->
  case erlang:get(Key) of
    undefined -> not_found;
    Item -> Item
  end.