%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc Sorted Sets
%%% @end
%%%-------------------------------------------------------------------
-module(zsets).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-record(zset, {dict :: dict(),
               tree :: gb_tree()}).
-opaque zset(_Scores, _Values) :: #zset{}.

-export_type([zset/2]).

-export([new/0, enter/2, enter/3, size/1]).

%% @doc Creates an empty {@link zset(any(), any())}
-spec new() -> zset(any(), any()).
new() ->
  #zset{dict = dict:new(), tree = gb_trees:empty()}.

%% @equiv enter(Score, Value, ZSet)
-spec enter({Score, Value}, zset(Scores, Values)) -> zset(Scores, Values) when is_subtype(Score, Scores), is_subtype(Value, Values).
enter({Score, Value}, ZSet) ->
  enter(Score, Value, ZSet).

%% @doc Adds a new element to the zset.
%%      If the element is already present it justs updates its score
-spec enter(Score, Value, zset(Scores, Values)) -> zset(Scores, Values) when is_subtype(Score, Scores), is_subtype(Value, Values).
enter(Score, Value, ZSet = #zset{}) ->
  case dict:find(Value, ZSet#zset.dict) of
    error ->
      ZSet#zset{dict = dict:store(Value, Score, ZSet#zset.dict),
                tree = gb_trees:enter({Score, Value}, Value, ZSet#zset.tree)};
    {ok, PrevScore} ->
      ZSet#zset{dict = dict:store(Value, Score, ZSet#zset.dict),
                tree = gb_trees:enter({Score, Value}, Value,
                                      gb_trees:delete({PrevScore, Value}, ZSet#zset.tree))}
  end.

%% @doc Returns the size of the zset
-spec size(zset(any(), any())) -> non_neg_integer().
size(ZSet) ->
  gb_trees:size(ZSet#zset.tree).

x() -> y.