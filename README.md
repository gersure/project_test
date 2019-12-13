# test code for some project

### 1. rocksdb_metrics
这个项目是基于prometheus和grafana写的rocksdb关键metrics的可视化项目;<br>
可以通过grafana面板直观展示各种rocksdb的参数的影响;包括:
* 调试合适的参数防止写停滞
* 多column family效率
* 多rocksdb实例效率
* batch和非batch的效率对比
* 等等,可以自由添加

注: 需要引用prometheus库: https://github.com/gersure/prometheus-cpp; 因为源prometheus有些用法不是很高效<br>
![images](https://github.com/gersure/project_test/blob/master/rocksdb_metrics/dashboard.png)
