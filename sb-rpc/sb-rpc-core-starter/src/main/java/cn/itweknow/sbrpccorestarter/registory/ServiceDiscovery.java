package cn.itweknow.sbrpccorestarter.registory;

import cn.itweknow.sbrpccorestarter.common.Constants;
import cn.itweknow.sbrpccorestarter.exception.ZkConnectException;
import cn.itweknow.sbrpccorestarter.model.ProviderInfo;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * @author ganchaoyang
 * @date 2018/10/26 17:27
 * @description
 */
public class ServiceDiscovery {

    private Logger logger = LoggerFactory.getLogger(ServiceDiscovery.class);

    private volatile Map<String,List<ProviderInfo>> providerMap = new HashMap<>();
    private volatile List<ProviderInfo> dataList = new ArrayList<>();
    private volatile int index=0;

    public ServiceDiscovery(String registoryAddress) throws ZkConnectException {
        try {
            // 获取zk连接。
            ZooKeeper zooKeeper = new ZooKeeper(registoryAddress, 2000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    logger.info("consumer connect zk status:{},event:{}",event.getState().name(), event.getType().name());
                }
            });
            watchNode(zooKeeper);
        } catch (Exception e) {
            throw new ZkConnectException("connect to zk exception," + e.getMessage(), e.getCause());
        }
    }

    public void watchNode(final ZooKeeper zk) {
        try {
            List<String> nodeList = zk.getChildren(Constants.ZK_ROOT_DIR, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    // 节点改变，有服务上线或下线
                    logger.info("consumer connect zk status:{},event:{}",event.getState().name(), event.getType().name());
                    if (event.getType().equals(Event.EventType.NodeChildrenChanged)) {
                        watchNode(zk);
                    }
                }
            });
            List<ProviderInfo> providerInfos = new ArrayList<>();
            // 循环子节点，获取服务名称
            for (String node: nodeList) {
                byte[] bytes = zk.getData(Constants.ZK_ROOT_DIR + "/" + node, false, null);//"/rpc/"
                String[] providerInfo = new String(bytes).split(",");
                if (providerInfo.length == 2) {
                    providerInfos.add(new ProviderInfo(providerInfo[0], providerInfo[1]));
                }
            }
            this.dataList = providerInfos;
//            collectToHash();
            logger.info("RpcStarter::ServiceDiscovery:获取服务端列表成功：{}", this.dataList);
        } catch (Exception e) {
            logger.error("RpcStarter::ServiceDiscovery:watch zk error,", e);
        }
    }

    void collectToHash(){
        providerMap.clear();
        for (ProviderInfo providerInfo : dataList) {
            List<ProviderInfo> providerInfoList = providerMap.get(providerInfo.getName());
            if (providerInfoList != null){
                providerInfoList = new ArrayList<>();
                providerMap.put(providerInfo.getName(), providerInfoList);
            }
            providerInfoList.add(providerInfo);
        }
    }

    /**
     * 获取一个服务提供者
     * @param providerName
     * @return
     */
    public ProviderInfo discover(String providerName) {
        if (dataList.isEmpty()) {
            return null;
        }
        List<ProviderInfo> providerInfos = dataList.stream()
                .filter(one -> providerName.equals(one.getName()))
                .collect(Collectors.toList());
        if (providerInfos.isEmpty()) {
            return null;
        }

        return providerInfos.get(ThreadLocalRandom.current()
                .nextInt(providerInfos.size()));
//        return providerInfos.get(next()%providerInfos.size());
    }

    private int next(){
        return ++index;
    }
}
