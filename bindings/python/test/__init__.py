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
import pymongo


class ClientContext:
    def __init__(self, host="localhost", port=27017):
        self.host = host
        self.port = port
        self.client = None
        self.connected = False

    def get_client(self, **args):
        kwargs = {"directConnection": False}
        kwargs.update(args)
        return pymongo.MongoClient(self.host, self.port, **kwargs)

    def init(self):
        client = pymongo.MongoClient(self.host, self.port, serverSelectionTimeoutMS=5000)
        try:
            client.admin.command("isMaster")
        except (pymongo.errors.OperationFailure, pymongo.errors.ConnectionFailure):
            self.connected = False
        else:
            self.connected = True
        finally:
            client.close()

        if self.connected:
            self.client = self.get_client()


client_context = ClientContext()
client_context.init()
