package org.apache.storm;

import org.apache.thrift.TException;
import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

import java.io.*;

/**
 * Created by anirudhnair on 3/7/16.
 */
public class NodeStatServerImpl implements NodeStatServer.Iface {

    private  Sigar sigar;
    private  String sIP;

    public NodeStatServerImpl(String sIP)
    {
        sigar = new Sigar();
        this.sIP = sIP;

    }

    public double GetCPUUsage() throws TException
    {
        double cpu_perc_usage = 0.0;
        try {
            CpuPerc perc = sigar.getCpuPerc();
            cpu_perc_usage = perc.getCombined() * 100;
        } catch (Exception se) {
            return 0.0;
        }
        return cpu_perc_usage;
    }

    public double GetMemUsage() throws TException
    {
        double mem_perc_usage = 0.0;

        try    {
            Mem mem = sigar.getMem();
            long actualFree = mem.getActualFree();
            long actualUsed = mem.getActualUsed();
            long per1Mem = actualFree + actualUsed;
            mem_perc_usage = ((double)actualUsed / (double)per1Mem) * 100.0;
        } catch (SigarException se)
        {
            return 0.0;
        }
        return mem_perc_usage;
    }

    public double GetPowerUsage() throws TException {
        // generate command
        String outFile = "/home/ajayaku2/power/power_" + sIP;
        String cmd = Common.GeneratePowerCommand(sIP);
        cmd = cmd + " >" + outFile;
        if(!cmd.equals("NULL")) {
            //execute command
            try {
                Process p = Runtime.getRuntime().exec(new String[]{"bash","-c",cmd});
                Thread.sleep(1000);
            // read from file
                FileInputStream fstream = new FileInputStream(outFile);
                DataInputStream in = new DataInputStream(fstream);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                String strLine;
                strLine = br.readLine();
                strLine = strLine.replaceAll("\\s+","");
                int power = Integer.parseInt(strLine);
                return (double)power/10.0;
            } catch (IOException e) {
                e.printStackTrace();
                return 0.0;
            } catch (InterruptedException e) {
                e.printStackTrace();
                return 0.0;
            }

            // return
        }
        else return 0.0;
    }
}
