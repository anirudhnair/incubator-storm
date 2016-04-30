namespace java org.apache.storm

typedef i32 int

service NodeStatServer{

    double GetCPUUsage();
    double GetMemUsage();
    double GetPowerUsage();
}
