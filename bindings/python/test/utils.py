# Copyright 2021-present MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from collections import defaultdict

from pymongo import monitoring


class EventListener(monitoring.CommandListener):
    def __init__(self):
        self.results = defaultdict(list)

    def started(self, event):
        self.results["started"].append(event)

    def succeeded(self, event):
        self.results["succeeded"].append(event)

    def failed(self, event):
        self.results["failed"].append(event)

    def started_command_names(self):
        """Return list of command names started."""
        return [event.command_name for event in self.results["started"]]

    def reset(self):
        """Reset the state of this listener."""
        self.results.clear()


class AllowListEventListener(EventListener):
    def __init__(self, *commands):
        self.commands = set(commands)
        super(AllowListEventListener, self).__init__()

    def started(self, event):
        if event.command_name in self.commands:
            super(AllowListEventListener, self).started(event)

    def succeeded(self, event):
        if event.command_name in self.commands:
            super(AllowListEventListener, self).succeeded(event)

    def failed(self, event):
        if event.command_name in self.commands:
            super(AllowListEventListener, self).failed(event)
