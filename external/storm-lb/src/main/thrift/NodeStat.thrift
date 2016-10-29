namespace java org.apache.storm

typedef i32 int

struct ThriftStat {
    1: required double cpu;
    2: required double mem;
    3: required double pow;
    4: required double net;

}

service NodeStatServer{

    double GetCPUUsage();
    double GetMemUsage();
    double GetPowerUsage();
    double GetNetUsage();
    ThriftStat GetStat();
}
