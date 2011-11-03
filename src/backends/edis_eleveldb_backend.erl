%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc leveldb based backend
%%% @end
%%%-------------------------------------------------------------------
-module(edis_eleveldb_backend).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(edis_backend).

-include("edis.hrl").

-record(ref, {db    :: eleveldb:db_ref(),
              file  :: string()}).
-opaque ref() :: #ref{}.
-export_type([ref/0]).

-export([init/3, write/2, put/3, delete/2, fold/3, is_empty/1, destroy/1, status/1, get/2]).

%% ====================================================================
%% Behaviour functions
%% ====================================================================
-spec init(string(), non_neg_integer(), eleveldb:open_options()) -> {ok, ref()} | {error, term()}.
init(Dir, Index, Options) ->
  File = Dir ++ "/edis-" ++ integer_to_list(Index),
  case eleveldb:open(File, Options) of
    {ok, Db} -> {ok, #ref{db = Db, file = File}};
    Error -> Error
  end.

-spec write(ref(), edis_backend:write_actions()) -> ok | {error, term()}.
write(#ref{db = Db}, Actions) ->
  ParseAction = fun({put, Key, Item}) -> {put, Key, erlang:term_to_binary(Item)};
                   (Action) -> Action
                end,
  eleveldb:write(Db, lists:map(ParseAction, Actions), []).

-spec put(ref(), binary(), #edis_item{}) -> ok | {error, term()}.
put(#ref{db = Db}, Key, Item) ->
  eleveldb:put(Db, Key, erlang:term_to_binary(Item), []).

-spec delete(ref(), binary()) -> ok | {error, term()}.
delete(#ref{db = Db}, Key) ->
  eleveldb:delete(Db, Key, []).

-spec fold(ref(), edis_backend:fold_fun(), term()) -> term().
fold(#ref{db = Db}, Fun, InitValue) ->
  eleveldb:fold(
    Db,
    fun({_Key, Bin}, Acc) ->
      Fun(erlang:binary_to_term(Bin), Acc)
    end, InitValue, [{fill_cache, false}]).

-spec is_empty(ref()) -> boolean().
is_empty(#ref{db = Db}) -> eleveldb:is_empty(Db).

-spec destroy(ref()) -> ok | {error, term()}.
destroy(#ref{file = File}) -> eleveldb:destroy(File, []).

-spec status(ref()) -> {ok, binary()} | error.
status(#ref{db = Db}) -> eleveldb:status(Db, <<"leveldb.stats">>).

-spec get(ref(), binary()) -> #edis_item{} | not_found | {error, term()}.
get(#ref{db = Db}, Key) ->
  case eleveldb:get(Db, Key, []) of
    {ok, Bin} -> erlang:binary_to_term(Bin);
    NotFoundOrError -> NotFoundOrError
  end.