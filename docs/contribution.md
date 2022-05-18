<!--
 Copyright 2021 Alibaba Group Holding Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
-->

# Contribution Guide

This project will be improved continuously. We welcome any feedback and contribution to this
project.

## Code Style

This project adopts similar code style with Flink. You can run the following command to format the
code after you have made some changes.

```bash
mvn spotless:apply
```

To improve readability, commit messages should use the following format:

```
[FRS-<issue id>][module name] commit message
```

Here is a simple example: \[FRS-100]\[docs] Improve document of deployment.

## How to Contribute

For collaboration, feel free to [contact us](../README.md#support)
. To report a bug, you can just open an issue on GitHub and attach the exceptions and your analysis
if any. For other improvements, you can contact us or open an issue first and describe what
improvement you would like to do. After reaching a consensus, you can open a pull request and your
pull request will get merged after reviewed.

## Improvements on the Schedule

There are already some further improvements on the schedule and welcome to contact us for
collaboration:

1. Introduce web UI and add more metrics for better usability.

2. Implement ReducePartition.

3. Support more storage backends.

4. More graceful upgrading.

5. Further performance improvements.

6. Production-ready standalone deployment.

7. Isolation and security enhancement.

8. Support adaptive execution.

and so on.
