package com.spnotes.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.*;
import java.net.URI;
import java.util.Scanner;

/**
 * Created by user on 7/1/14.
 */
public class HelloHDFS {

    public static void main(String[] argv) throws Exception{
        if(argv.length != 2){
            System.out.println("Use HelloHDFS read/write/replicate path");
            System.exit(-1);
        }
        String command = argv[0];
        String filePath = argv[1];
        Configuration conf = new Configuration();
        DistributedFileSystem dfs = (DistributedFileSystem)FileSystem.get(URI.create(filePath),conf);
        HelloHDFS hdfsClient = new HelloHDFS();
        if(command.equals("read")){
            hdfsClient.read(dfs,filePath);
        }else if(command.equals("write")){
            hdfsClient.write(dfs, filePath);
        }else if(command.equals("replicate")){
            hdfsClient.setReplication(dfs,filePath);
        }
    }

    private void read(DistributedFileSystem fs, String path)throws IOException{
        System.out.println("Entering HelloHDFS.read()");
        InputStream in = null;
        try{
            in = fs.open(new Path(path) );
            IOUtils.copy(in, System.out);
        }finally{
            IOUtils.closeQuietly(in);
        }
        System.out.println("Entering HelloHDFS.read()");
    }

    private void write(DistributedFileSystem fs, String filePath)throws IOException{
        System.out.println("Entering HelloHDFS.write()");

        OutputStreamWriter out = null;
        try{
             out =  new OutputStreamWriter(fs.create(new Path(filePath)));
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String line = br.readLine();
             while(line != null){
                if(line.equals("endfile")){
                    break;
                }
                line = br.readLine();
               IOUtils.write(line,out);
            }
        }finally{
            out.close();
        }
        System.out.println("Entering HelloHDFS.wite()");
    }

    private void setReplication(DistributedFileSystem dfs, String filePath)throws IOException{
        System.out.println("Entering HelloHDFS.setReplicationFactor()");
        Scanner reader = new Scanner(System.in);

        System.out.println("Enter replication factor:");
        short replicationFactor = reader.nextShort();
        dfs.setReplication(new Path(filePath),replicationFactor);
        System.out.println("Entering HelloHDFS.setReplicationFactor()");

    }


}
