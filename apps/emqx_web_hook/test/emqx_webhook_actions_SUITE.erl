%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_webhook_actions_SUITE).
-include_lib("eunit/include/eunit.hrl").
-compile([export_all, nowarn_export_all]).
%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Cfg) ->
    emqx_ct_helpers:start_apps([emqx, emqx_rule_engine, emqx_management]),
    Cfg.

end_per_suite(Cfg) ->
    emqx_ct_helpers:stop_apps([emqx_management, emqx_rule_engine, emqx]),
    Cfg.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------
t_export(_) ->
    {ok, _} = emqx_mgmt_data_backup:export().

t_import_bridge(_) ->
    Path = emqx_ct_helpers:deps_path(emqx_web_hook, "test/emqx_web_hook_SUITE_data/"),
    ok = emqx_mgmt_data_backup:import(Path ++ "/4_2_bridge.json").

t_import_webhook(_) ->
    Path = emqx_ct_helpers:deps_path(emqx_web_hook, "test/emqx_web_hook_SUITE_data/"),
    ok = emqx_mgmt_data_backup:import(Path ++ "/4_2_webhook.json").
