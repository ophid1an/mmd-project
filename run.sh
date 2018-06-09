#!/bin/bash
spark-submit --jars lib/scopt_native0.3_2.11-3.7.0.jar target/scala-2.11/mmd-project_2.11-0.1.jar "$@"

