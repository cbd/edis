%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc leveldb based backend
%%% @end
%%%-------------------------------------------------------------------
-module(edis_hanoidb_backend).
-author('Greg Burd <greg@burd.me>').

-behaviour(edis_backend).

-include("edis.hrl").
-include_lib("hanoidb/include/hanoidb.hrl").

-record(ref, {db    :: hanoidb:db_ref(),
              file  :: string()}).
-opaque ref() :: #ref{}.
-export_type([ref/0]).

-export([init/3, write/2, put/3, delete/2, fold/3, is_empty/1, destroy/1, status/1, get/2]).

%% ====================================================================
%% Behaviour functions
%% ====================================================================
-spec init(string(), non_neg_integer(), hanoidb:open_options()) -> {ok, ref()} | {error, term()}.
init(Dir, Index, Options) ->
  File = Dir ++ "/edis-" ++ integer_to_list(Index),
  case hanoidb:open(File, Options) of
    {ok, Db} -> {ok, #ref{db = Db, file = File}};
    Error -> Error
  end.

-spec write(ref(), edis_backend:write_actions()) -> ok | {error, term()}.
write(#ref{db = Db}, Actions) ->
  ParseAction = fun({put, Key, Item}) ->
                        {put, sext:encode(Key), erlang:term_to_binary(Item)};
                   ({delete, Key}) ->
                        {delete, sext:encode(Key)};
                   (Action) -> Action
                end,
  hanoidb:transact(Db, lists:map(ParseAction, Actions)).

-spec put(ref(), binary(), #edis_item{}) -> ok | {error, term()}.
put(#ref{db = Db}, Key, Item) ->
  hanoidb:put(Db, sext:encode(Key), erlang:term_to_binary(Item)).

-spec delete(ref(), binary()) -> ok | {error, term()}.
delete(#ref{db = Db}, Key) ->
  hanoidb:delete(Db, sext:encode(Key)).

-spec fold(ref(), edis_backend:fold_fun(), term()) -> term().
fold(#ref{db = Db}, Fun, InitValue) ->
  hanoidb:range(
    Db,
    fun({_Key, Bin}, Acc) ->
      Fun(erlang:binary_to_term(Bin), Acc)
    end,
    InitValue).

-define(MAX_OBJECT_KEY, <<16,0,0,0,4>>). %% TODO: really?

-spec is_empty(ref()) -> boolean().
is_empty(#ref{db = Db}) ->
    FoldFun = fun(_K, _V, _Acc) -> throw(ok) end,
    try
        Range = #key_range{ from_key = sext:decode(<<>>),
                  from_inclusive = true,
                  to_key         = ?MAX_OBJECT_KEY,
                  to_inclusive   = false
                },
        [] =:= hanoi:fold_range(Db, FoldFun, [], Range)
    catch
        _:ok ->
            false
    end.

-spec destroy(ref()) -> ok | {error, term()}.
destroy(#ref{file = _File}) ->
    throw(not_implemented). %% TODO: not yet implemented

-spec status(ref()) -> {ok, binary()} | error.
status(#ref{db = _Db}) ->
    throw(not_implemented). %% TODO: not yet implemented

-spec get(ref(), binary()) -> #edis_item{} | not_found | {error, term()}.
get(#ref{db = Db}, Key) ->
    case hanoidb:get(Db, sext:encode(Key)) of
        {ok, Bin} ->
            erlang:binary_to_term(Bin);
        NotFoundOrError ->
            NotFoundOrError
    end.
