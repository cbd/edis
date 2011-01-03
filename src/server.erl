-module(server).
-compile(export_all).

-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

%% Public API

start() ->
  gen_server:start({local, ?MODULE}, ?MODULE, [], []).

stop(Module) ->
  gen_server:call(Module, stop).

stop() ->
  stop(?MODULE).

state(Module) ->
  gen_server:call(Module, state).

state() ->
  state(?MODULE).

%% Server implementation, a.k.a.: callbacks

init([]) ->
  debug:log("init", []),
  {ok, []}.

handle_call(stop, _From, State) ->
  debug:log("stopping by ~p, state was ~p.", [_From, State]),
  {stop, normal, stopped, State};

handle_call(state, _From, State) ->
  debug:log("~p is asking for the state.", [_From]),
  {reply, State, State};

handle_call(_Request, _From, State) ->
  debug:log("call ~p, ~p, ~p.", [_Request, _From, State]),
  {reply, ok, State}.

handle_cast(_Msg, State) ->
  debug:log("cast ~p, ~p.", [_Msg, State]),
  {noreply, State}.

handle_info(_Info, State) ->
  debug:log("info ~p, ~p.", [_Info, State]),
  {noreply, State}.

terminate(_Reason, _State) ->
  debug:log("terminate ~p, ~p", [_Reason, _State]),
  ok.

code_change(_OldVsn, State, _Extra) ->
  debug:log("code_change ~p, ~p, ~p", [_OldVsn, State, _Extra]),
  {ok, State}.


