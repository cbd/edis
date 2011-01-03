-module(edis_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, boot/0]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

boot() -> 
  application:start(edis).

start(_StartType, _StartArgs) ->
    edis_sup:start_link().

stop(_State) ->
    ok.
