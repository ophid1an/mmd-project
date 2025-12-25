#!/bin/bash
spark-submit --packages com.github.scopt:scopt_2.13:4.1.0 target/scala-2.13/mmd-project_2.13-0.1.jar "$@"
