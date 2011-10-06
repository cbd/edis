%%%-------------------------------------------------------------------
%%% @author Fernando Benavides <fernando.benavides@inakanetworks.com>
%%% @author Chad DePue <chad@inakanetworks.com>
%%% @copyright (C) 2011 InakaLabs SRL
%%% @doc edis Database
%%% @todo It's currently delivering all operations to the leveldb instance, i.e. no in-memory management
%%%       Therefore, operations like save/1 are not really implemented
%%% @todo We need to evaluate which calls should in fact be casts
%%% @todo We need to add info to INFO
%%% @end
%%%-------------------------------------------------------------------
-module(edis_db).
-author('Fernando Benavides <fernando.benavides@inakanetworks.com>').
-author('Chad DePue <chad@inakanetworks.com>').

-behaviour(gen_server).

-include("edis.hrl").
-define(DEFAULT_TIMEOUT, 5000).

-record(state, {index     :: non_neg_integer(),
                db        :: eleveldb:db_ref(),
                last_save :: float()}).
-opaque state() :: #state{}.

%% Administrative functions
-export([start_link/1, process/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Commands ========================================================================================
-export([ping/1, save/1, last_save/1, info/1, flush/0, flush/1, size/1]).
-export([append/3, decr/3, get/2, get_bit/3, get_range/4, get_and_set/3, incr/3, set/2, set/3,
         set_nx/2, set_nx/3, set_bit/4, set_ex/4, set_range/4, str_len/2]).
-export([del/2]).

%% =================================================================================================
%% External functions
%% =================================================================================================
-spec start_link(non_neg_integer()) -> {ok, pid()}.
start_link(Index) ->
  gen_server:start_link({local, process(Index)}, ?MODULE, Index, []).

-spec process(non_neg_integer()) -> atom().
process(Index) ->
  list_to_atom("edis-db-" ++ integer_to_list(Index)).

%% =================================================================================================
%% Commands
%% =================================================================================================
-spec size(atom()) -> non_neg_integer().
size(Db) ->
  make_call(Db, size).

-spec flush() -> ok.
flush() ->
  lists:foreach(
    fun flush/1, [process(Index) || Index <- lists:seq(0, edis_config:get(databases) - 1)]).

-spec flush(atom()) -> ok.
flush(Db) ->
  make_call(Db, flush).

-spec ping(atom()) -> pong.
ping(Db) ->
  make_call(Db, ping).

-spec save(atom()) -> ok.
save(Db) ->
  make_call(Db, save).

-spec last_save(atom()) -> ok.
last_save(Db) ->
  make_call(Db, last_save).

-spec info(atom()) -> [{atom(), term()}].
info(Db) ->
  make_call(Db, info).

-spec append(atom(), binary(), binary()) -> pos_integer().
append(Db, Key, Value) ->
  make_call(Db, {append, Key, Value}).

-spec decr(atom(), binary(), integer()) -> integer().
decr(Db, Key, Decrement) ->
  make_call(Db, {decr, Key, Decrement}).

-spec get(atom(), binary()|[binary()]) -> undefined | binary().
get(Db, Key) when is_binary(Key) ->
  [Value] = get(Db, [Key]),
  Value;
get(Db, Keys) ->
  make_call(Db, {get, Keys}).

-spec get_bit(atom(), binary(), non_neg_integer()) -> 1|0.
get_bit(Db, Key, Offset) ->
  make_call(Db, {get_bit, Key, Offset}).

-spec get_range(atom(), binary(), integer(), integer()) -> binary().
get_range(Db, Key, Start, End) ->
  make_call(Db, {get_range, Key, Start, End}).

-spec get_and_set(atom(), binary(), binary()) -> undefined | binary().
get_and_set(Db, Key, Value) ->
  make_call(Db, {get_and_set, Key, Value}).

-spec incr(atom(), binary(), integer()) -> integer().
incr(Db, Key, Increment) ->
  make_call(Db, {incr, Key, Increment}).

-spec set(atom(), binary(), binary()) -> ok.
set(Db, Key, Value) ->
  set(Db, [{Key, Value}]).

-spec set(atom(), [{binary(), binary()}]) -> ok.
set(Db, KVs) ->
  make_call(Db, {set, KVs}).

-spec set_nx(atom(), binary(), binary()) -> ok.
set_nx(Db, Key, Value) ->
  set_nx(Db, [{Key, Value}]).

-spec set_nx(atom(), [{binary(), binary()}]) -> ok.
set_nx(Db, KVs) ->
  make_call(Db, {set_nx, KVs}).

-spec set_bit(atom(), binary(), non_neg_integer(), 1|0) -> 1|0.
set_bit(Db, Key, Offset, Bit) ->
  make_call(Db, {set_bit, Key, Offset, Bit}).

-spec set_ex(atom(), binary(), pos_integer(), binary()) -> ok.
set_ex(Db, Key, Seconds, Value) ->
  make_call(Db, {set_ex, Key, Seconds, Value}).

-spec set_range(atom(), binary(), pos_integer(), binary()) -> non_neg_integer().
set_range(Db, Key, Offset, Value) ->
  make_call(Db, {set_range, Key, Offset, Value}).

-spec str_len(atom(), binary()) -> non_neg_integer().
str_len(Db, Key) ->
  make_call(Db, {str_len, Key}).

-spec del(atom(), binary()) -> non_neg_integer().
del(Db, Keys) ->
  make_call(Db, {del, Keys}).

%% =================================================================================================
%% Server functions
%% =================================================================================================
%% @hidden
-spec init(non_neg_integer()) -> {ok, state()} | {stop, any()}.
init(Index) ->
  case eleveldb:open("db/edis-" ++ integer_to_list(Index), [{create_if_missing, true}]) of
    {ok, Ref} ->
      {ok, #state{index = Index, db = Ref, last_save = edis_util:timestamp()}};
    {error, Reason} ->
      ?THROW("Couldn't start level db #~p:~b\t~p~n", [Index, Reason]),
      {stop, Reason}
  end.

%% @hidden
-spec handle_call(term(), reference(), state()) -> {reply, ok | {ok, term()} | {error, term()}, state()} | {stop, {unexpected_request, term()}, {unexpected_request, term()}, state()}.
handle_call(save, _From, State) ->
  {reply, ok, State#state{last_save = edis_util:timestamp()}};
handle_call(last_save, _From, State) ->
  {reply, {ok, State#state.last_save}, State};
handle_call(ping, _From, State) ->
  {reply, {ok, pong}, State};
handle_call(info, _From, State) ->
  Version =
    case lists:keyfind(edis, 1, application:loaded_applications()) of
      false -> "0";
      {edis, _Desc, V} -> V
    end,
  {ok, Stats} = eleveldb:status(State#state.db, <<"leveldb.stats">>),
  {reply, {ok, [{edis_version, Version},
                {last_save, State#state.last_save},
                {db_stats, Stats}]}, %%TODO: add info
   State};
handle_call(flush, _From, State) ->
  ok = eleveldb:destroy("db/edis-" ++ integer_to_list(State#state.index), []),
  case init(State#state.index) of
    {ok, NewState} ->
      {reply, ok, NewState};
    {stop, Reason} ->
      {reply, {error, Reason}, State}
  end;
handle_call(size, _From, State) ->
  %%TODO: Is there any way to improve this?
  Size = eleveldb:fold_keys(
           State#state.db, fun(_, Acc) -> Acc + 1 end, 0,
           [{verify_checksums, false}]),
  {reply, {ok, Size}, State};
handle_call({append, Key, Value}, _From, State) ->
  Reply =
    update(State#state.db, Key, string,
           fun(Item = #edis_item{value = OldV}) ->
                   NewV = <<OldV/binary, Value/binary>>,
                   {erlang:size(NewV), Item#edis_item{value = NewV}}
           end, <<>>),
  {reply, Reply, State};
handle_call({decr, Key, Decrement}, _From, State) ->
  Reply =
    update(State#state.db, Key, string,
           fun(Item = #edis_item{value = OldV}) ->
                   try edis_util:binary_to_integer(OldV) of
                     OldInt ->
                       Res = OldInt - Decrement,
                       {Res, Item#edis_item{value = edis_util:integer_to_binary(Res)}}
                   catch
                     _:badarg ->
                       throw(bad_item_type)
                   end
           end, <<"0">>),
  {reply, Reply, State};
handle_call({get, Keys}, _From, State) ->
  Reply =
    lists:foldr(
      fun(Key, {ok, AccValues}) ->
              case get_item(State#state.db, string, Key) of
                #edis_item{type = string, value = Value} ->
                  {ok, [Value | AccValues]};
                not_found ->
                  {ok, [undefined | AccValues]};
                {error, bad_item_type} ->
                  {ok, [undefined | AccValues]};
                {error, Reason} ->
                  {error, Reason}
              end;
         (_, AccErr) ->
              AccErr
      end, {ok, []}, Keys),
  {reply, Reply, State};
handle_call({get_bit, Key, Offset}, _From, State) ->
  case get_item(State#state.db, string, Key) of
    #edis_item{value = <<_:Offset/unit:1, Bit:1/unit:1, _Rest/bitstring>>} ->
      {reply, {ok, Bit}, State};
    #edis_item{} -> %% Value is shorter than offset
      {reply, {ok, 0}, State};
    not_found ->
      {reply, {ok, 0}, State};
    {error, Reason} ->
      {reply, {error, Reason}, State}
  end;
handle_call({get_range, Key, Start, End}, _From, State) ->
  try
    case get_item(State#state.db, string, Key) of
      #edis_item{value = Value} ->
        L = erlang:size(Value),
        StartPos =
          case Start of
            Start when Start >= L -> throw(empty);
            Start when Start >= 0 -> Start;
            Start when Start < (-1)*L -> 0;
            Start -> L + Start
          end,
        EndPos =
          case End of
            End when End >= 0, End >= L -> L - 1;
            End when End >= 0 -> End;
            End when End < (-1)*L -> 0;
            End -> L + End
          end,
        case EndPos - StartPos + 1 of
          Len when Len =< 0 ->
            {reply, {ok, <<>>}, State};
          Len ->
            {reply, {ok, binary:part(Value, StartPos, Len)}, State}
        end;
      not_found ->
        {reply, {ok, <<>>}, State};
      {error, Reason} ->
        {reply, {error, Reason}, State}
    end
  catch
    _:empty ->
      {reply, {ok, <<>>}, State}
  end;
handle_call({get_and_set, Key, Value}, _From, State) ->
  Reply =
    update(State#state.db, Key, string,
           fun(Item = #edis_item{value = OldV}) ->
                   {OldV, Item#edis_item{value = Value}}
           end, undefined),
  {reply, Reply, State};
handle_call({incr, Key, Increment}, _From, State) ->
  Reply =
    update(State#state.db, Key, string,
           fun(Item = #edis_item{value = OldV}) ->
                   try edis_util:binary_to_integer(OldV) of
                     OldInt ->
                       Res = OldInt + Increment,
                       {Res, Item#edis_item{value = edis_util:integer_to_binary(Res)}}
                   catch
                     _:badarg ->
                       throw(bad_item_type)
                   end
           end, <<"0">>),
  {reply, Reply, State};
handle_call({set, KVs}, _From, State) ->
  Reply =
    eleveldb:write(State#state.db,
                   [{put, Key,
                     erlang:term_to_binary(
                       #edis_item{key = Key, type = string, value = Value})} || {Key, Value} <- KVs],
                    []),
  {reply, Reply, State};
handle_call({set_nx, KVs}, _From, State) ->
  case lists:any(
         fun({Key, _}) ->
                 exists(State#state.db, Key)
         end, KVs) of
    true ->
      {reply, {error, already_exists}, State};
    false ->
      Reply =
        eleveldb:write(State#state.db,
                       [{put, Key,
                         erlang:term_to_binary(
                           #edis_item{key = Key, type = string, value = Value})} || {Key, Value} <- KVs],
                       []),
      {reply, Reply, State}
  end;
handle_call({set_bit, Key, Offset, Bit}, _From, State) ->
  Reply =
    update(State#state.db, Key, string,
           fun(Item = #edis_item{value = <<Prefix:Offset/unit:1, OldBit:1/unit:1, _Rest/bitstring>>}) ->
                   {OldBit,
                    Item#edis_item{value = <<Prefix:Offset/unit:1, Bit:1/unit:1, _Rest/bitstring>>}};
              (Item) when Bit == 0 -> %% Value is shorter than offset
                   {0, Item};
              (Item = #edis_item{value = Value}) when Bit == 1 -> %% Value is shorter than offset
                   BitsBefore = Offset - (erlang:size(Value) * 8),
                   BitsAfter = 7 - (Offset rem 8),
                   {0, Item#edis_item{value = <<Value/bitstring, 
                                                0:BitsBefore/unit:1,
                                                1:1/unit:1,
                                                0:BitsAfter/unit:1>>}}
           end, <<>>),
  {reply, Reply, State};
handle_call({set_ex, Key, Seconds, Value}, _From, State) ->
  Reply =
    eleveldb:put(
      State#state.db, Key,
      erlang:term_to_binary(
        #edis_item{key = Key, type = string,
                   expire = edis_util:now() + Seconds,
                   value = Value}), []),
  {reply, Reply, State};
handle_call({set_range, Key, Offset, Value}, _From, State) ->
  case erlang:size(Value) of
    0 ->
      {reply, {ok, 0}, State}; %% Copying redis behaviour even when documentation said different
    Length ->
      Reply =
        update(State#state.db, Key, string,
               fun(Item = #edis_item{value = <<Prefix:Offset/binary, _:Length/binary, Suffix/binary>>}) ->
                       NewV = <<Prefix/binary, Value/binary, Suffix/binary>>,
                       {erlang:size(NewV), Item#edis_item{value = NewV}};
                  (Item = #edis_item{value = <<Prefix:Offset/binary, _/binary>>}) ->
                       NewV = <<Prefix/binary, Value/binary>>,
                       {erlang:size(NewV), Item#edis_item{value = NewV}};
                  (Item = #edis_item{value = Prefix}) ->
                       Pad = Offset - erlang:size(Prefix),
                       NewV = <<Prefix/binary, 0:Pad/unit:8, Value/binary>>,
                       {erlang:size(NewV), Item#edis_item{value = NewV}}
               end, <<>>),
      {reply, Reply, State}
  end;
handle_call({str_len, Key}, _From, State) ->
  case get_item(State#state.db, string, Key) of
    #edis_item{value = Value} ->
      {reply, {ok, erlang:size(Value)}, State};
    not_found ->
      {reply, {ok, 0}, State};
    {error, Reason} ->
      {reply, {error, Reason}, State}
  end;
handle_call({del, Keys}, _From, State) ->
  DeleteActions =
      [{delete, Key} || Key <- Keys, exists(State#state.db, Key)],
  case eleveldb:write(State#state.db, DeleteActions, []) of
    ok ->
      {reply, {ok, length(DeleteActions)}, State};
    {error, Reason} ->
      {reply, {error, Reason}, State}
  end;
handle_call(X, _From, State) ->
  {stop, {unexpected_request, X}, {unexpected_request, X}, State}.

%% @hidden
-spec handle_cast(X, state()) -> {stop, {unexpected_request, X}, state()}.
handle_cast(X, State) -> {stop, {unexpected_request, X}, State}.

%% @hidden
-spec handle_info(term(), state()) -> {noreply, state(), hibernate}.
handle_info(_, State) -> {noreply, State, hibernate}.

%% @hidden
-spec terminate(term(), state()) -> ok.
terminate(_, _) -> ok.

%% @hidden
-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%% =================================================================================================
%% Private functions
%% =================================================================================================
%% @private
exists(Db, Key) ->
  case eleveldb:get(Db, Key, []) of
    {ok, _} -> true;
    not_found -> false;
    {error, Reason} -> {error, Reason}
  end.

%% @private
get_item(Db, Type, Key) ->
  case eleveldb:get(Db, Key, []) of
    {ok, Bin} ->
      Now = edis_util:now(),
      case erlang:binary_to_term(Bin) of
        Item = #edis_item{type = T, expire = Expire}
          when Type =:= any orelse T =:= Type ->
          case Expire of
            Expire when Expire >= Now ->
              Item;
            _ ->
              _ = eleveldb:delete(Db, Key, []),
              not_found
          end;
        _Other -> {error, bad_item_type}
      end;
    not_found ->
      not_found;
    {error, Reason} ->
      {error, Reason}
  end.

%% @private
update(Db, Key, Type, Fun, Default) ->
  try
    {Res, NewItem} =
      case get_item(Db, Type, Key) of
        not_found ->
          Fun(#edis_item{key = Key, type = Type, value = Default});
        {error, Reason} ->
          throw(Reason);
        Item ->
          Fun(Item)
      end,
    case eleveldb:put(Db, Key, erlang:term_to_binary(NewItem), []) of
      ok -> {ok, Res};
      {error, Reason2} -> {error, Reason2}
    end
  catch
    _:Error ->
      {error, Error}
  end.

%% @private
make_call(Process, Request) ->
  make_call(Process, Request, ?DEFAULT_TIMEOUT).

%% @private
make_call(Process, Request, Timeout) ->
  ?DEBUG("CALL for ~p: ~p~n", [Process, Request]),
  ok = edis_db_monitor:notify(Process, Request),
  case gen_server:call(Process, Request, Timeout) of
    ok -> ok;
    {ok, Reply} -> Reply;
    {error, Error} ->
      ?THROW("Error trying ~p on ~p:~n\t~p~n", [Request, Process, Error]),
      throw(Error)
  end.