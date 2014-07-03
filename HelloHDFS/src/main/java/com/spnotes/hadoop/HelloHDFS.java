package com.spnotes.hadoop;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
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
        FileSystem fs = FileSystem.get(URI.create(filePath),conf);
        HelloHDFS hdfsClient = new HelloHDFS();
        if(command.equals("read")){
            hdfsClient.read(fs,filePath);
        }else if(command.equals("create")){
            hdfsClient.create(fs, filePath);
        }else if(command.equals("append")){
            hdfsClient.append(fs, filePath);
        }else if(command.equals("replicate")){
            hdfsClient.setReplication(fs,filePath);
        }else if(command.equals("status")){
            hdfsClient.getStatus(fs,filePath);
        }else if(command.equals("delete")){
            hdfsClient.delete(fs,filePath);
        }else if(command.equals("search")){
            hdfsClient.search(fs, filePath);
        }
    }

    private void read(FileSystem fs, String path)throws IOException{
        System.out.println("Entering HelloHDFS.read()");
        InputStream in = null;
        try{
            if(fs.exists(new Path(path)) != true)
                return;
            in = fs.open(new Path(path) );
            IOUtils.copy(in, System.out);
        }finally{
            IOUtils.closeQuietly(in);
        }
        System.out.println("Entering HelloHDFS.read()");
    }

    private void create(FileSystem fs, String filePath)throws IOException{
        System.out.println("Entering HelloHDFS.create()");

        OutputStreamWriter out = null;
        try{
             out =  new OutputStreamWriter(fs.create(new Path(filePath)));
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String line = br.readLine();
             while(line != null){
                 line = br.readLine();
                if(line.equals("endfile")){
                    break;
                }
               IOUtils.write(line,out);
            }
        }finally{
            out.close();
        }
        System.out.println("Entering HelloHDFS.create()");
    }

    private void append(FileSystem fs, String filePath)throws IOException{
        System.out.println("Entering HelloHDFS.append()");
        OutputStreamWriter out = null;
        try{
            out =  new OutputStreamWriter(fs.append(new Path(filePath)));
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            String line = br.readLine();
            while(line != null){

                line = br.readLine();
                if(line.equals("endfile")){
                    break;
                }
                IOUtils.write(line,out);
            }
        }finally{
            out.close();
        }

        System.out.println("Exiting HelloHDFS.append()");
    }

    private void getStatus(FileSystem fs, String filePath) throws IOException{
        System.out.println("Entering HelloHDFS.getStatus()");
        FileStatus[] fileStatusList = fs.listStatus(new Path(filePath));
        for(FileStatus fileStatus:fileStatusList){
            System.out.println(fileStatus);
        }
        System.out.println("Entering HelloHDFS.getStatus()");

    }


    private void delete(FileSystem fs, String filePath)throws IOException{
        System.out.println("Entering HelloHDFS.delete()");
        Path path = new Path(filePath);
        if(fs.exists(path)){
            fs.delete(path,true);
        }
        System.out.println("Exiting HelloHDFS.delete()");
    }


    private void setReplication(FileSystem dfs, String filePath)throws IOException{
        System.out.println("Entering HelloHDFS.setReplicationFactor()");
        Scanner reader = new Scanner(System.in);

        System.out.println("Enter replication factor:");
        short replicationFactor = reader.nextShort();
        dfs.setReplication(new Path(filePath),replicationFactor);
        System.out.println("Entering HelloHDFS.setReplicationFactor()");

    }

    private void search(FileSystem dfs, String filePattern)throws IOException{
        System.out.println("Entering HelloHDFS.search()");
        FileStatus[] fileStatusList= dfs.globStatus(new Path(filePattern));
        for(FileStatus fileStatus: fileStatusList){
            System.out.println(fileStatus);
        }
        System.out.println("Entering HelloHDFS.search()");

    }

}
