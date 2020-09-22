package cn.itweknow.sbrpcconsumer.controller;

import cn.itweknow.sbrpcapi.service.HelloRpcService;
import cn.itweknow.sbrpccorestarter.anno.RpcConsumer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/hello-rpc")
public class HelloRpcController {


    @RpcConsumer(providerName = "provider")
    private HelloRpcService helloRpcService;

    @GetMapping("/hello")
    public String hello(@RequestParam String msg) {
        long start = System.currentTimeMillis();
        String ret = helloRpcService.sayHello(msg);
        System.out.println("请求耗时:" + (System.currentTimeMillis()-start));
        return ret;

    }
}
