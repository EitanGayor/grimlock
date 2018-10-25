#!/bin/bash
#
# Copyright 2018 Commonwealth Bank of Australia
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

for file in $(ls demo.scalding)
do
  cat demo.scalding/$file | sort > x
  cat demo.spark/$file/* | sort > y

  echo "$file - $(wc -l x) | $(wc -l y)"
  diff x y
done

for file in $(ls tmp.scalding)
do
  cat tmp.scalding/$file | sort > x
  cat tmp.spark/$file/* | sort > y

  echo "$file - $(wc -l x) | $(wc -l y)"
  diff x y
done

rm x y
