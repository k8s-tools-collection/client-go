发现客户端，主要用于发现Kubenetes API Server所支持的资源组、资源版本、资源信息。 
kubectl的api-versions和api-resources命令输出也是通过DiscoveryClient实现的。
其同样是在RESTClient的基础上进行的封装。DiscoveryClient还可以将资源组、资源版本、
资源信息等存储在本地，用于本地缓存，减轻对kubernetes api sever的访问压力，
缓存信息默认存储在：~/.kube/cache和~/.kube/http-cache下