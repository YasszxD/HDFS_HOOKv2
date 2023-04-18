package org.example;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.jline.utils.Log;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.Map;

public class HdfsY {
    static FileSystem fileSystem = null;
    public static Configuration config = null;

    public HdfsY() throws IOException {
        connectHdfs();
    }

    public static void main(String[] args) throws IOException, MissingEventsException, InterruptedException {
        HdfsY hdfsY = new HdfsY();
       try {
            String filename = "cc11";
            writeFileToHDFS(filename);
            addAttr("/user/yexz/"+filename,"name",filename);
            addAttr("/user/yexz/"+filename,"displayName",filename);
            addAttr("/user/yexz/"+filename,"qualifiedName",filename+"@csv_files");
            Log.info("Added");
        } catch (IOException exception){}

        //readFileFromHDFS("10");



        // Start the hook
    }
    public static void connectHdfs() throws IOException {
        config = new Configuration();
        config.set("fs.defaultFS", "hdfs://hadoopnamenode:9000");
        FileSystem fileSystem = FileSystem.get(config);
    }
    public static void createDirectory(String pathToDir) throws IOException {
        fileSystem = FileSystem.get(config);
        String directoryName = pathToDir;
        Path path = new Path(directoryName);
        fileSystem.mkdirs(path);

    }
    public static void writeFileToHDFS(String fileName) throws IOException {
        fileSystem = FileSystem.get(config);
        //Create a path
        Path hdfsWritePath = new Path("/user/yexz/" + fileName);
        FSDataOutputStream fsDataOutputStream = fileSystem.create(hdfsWritePath,true);
        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream, StandardCharsets.UTF_8));
        bufferedWriter.write("Java API to write data in HDFS");
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    }
    public static void appendToHDFSFile(String fileName) throws IOException {
        FileSystem fileSystem = FileSystem.get(config);
        //Create a path
        Path hdfsWritePath = new Path("/user/yexz/" + fileName);

        FSDataOutputStream fsDataOutputStream = fileSystem.append(hdfsWritePath);

        BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(fsDataOutputStream,StandardCharsets.UTF_8));
        bufferedWriter.write("Java API to append data in HDFS file");
        bufferedWriter.newLine();
        bufferedWriter.close();
        fileSystem.close();
    } // tekhdmch
    public static String readFileFromHDFS(String fileName) throws IOException {

        FileSystem fileSystem = FileSystem.get(config);
        //Create a path
        Path hdfsReadPath = new Path(fileName);
        //Init input stream
        FSDataInputStream inputStream = fileSystem.open(hdfsReadPath);
        //Classical input stream usage
        String out= IOUtils.toString(inputStream, "UTF-8");
        inputStream.close();
        fileSystem.close();
        System.out.println(out);
        return out;/*
        System.out.println(out);
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8));
        String line = null;
        while ((line=bufferedReader.readLine())!=null){
            //System.out.println(line);
        }
        inputStream.close();
        fileSystem.close();*/
    }
    public static void checkExists() throws IOException {

        FileSystem fileSystem = FileSystem.get(config);
        String directoryName = "/user/yexz/javareadwriteexample/";
        Path path = new Path(directoryName);
        if(fileSystem.exists(path)){
            System.out.println("File/Folder Exists : "+path.getName());
        }else{
            System.out.println("File/Folder does not Exists : "+path.getName());
        }
    }
    public static void listMetadata() throws IOException{
        FileSystem fs = FileSystem.get(config);

// Path of the directory to list metadata for
        Path directoryPath = new Path("/");

// List all files and directories in the given directory
        FileStatus[] fileStatuses = fs.listStatus(directoryPath);

// Iterate over each file and directory and print its metadata
        for (FileStatus fileStatus : fileStatuses) {
            System.out.println("File/directory name: " + fileStatus.getPath().getName());
            System.out.println("File/directory path: " + fileStatus.getPath());
            System.out.println("File/directory size: " + fileStatus.getLen());
            System.out.println("File/directory owner: " + fileStatus.getOwner());
            System.out.println("File/directory group: " + fileStatus.getGroup());
            System.out.println("File/directory permissions: " + fileStatus.getPermission());
            System.out.println("File/directory modification time: " + new Date(fileStatus.getModificationTime()));
            System.out.println("File/directory access time: " + new Date(fileStatus.getAccessTime()));
            System.out.println("Is directory? " + fileStatus.isDirectory());
            System.out.println("Is file? " + fileStatus.isFile());
            System.out.println();
        }

// Close the file system
        fs.close();
    }


    public static void addAttr(String filePath, String name, String value) throws IOException {
        fileSystem = FileSystem.get(config);
        //String filePath = "/user/yexz/test1";
        Path path = new Path(filePath);
        //FileStatus fileStatus = fileSystem.getFileStatus(path);
        //name = "user.author";
        //String value = "John Doe";
        fileSystem.setXAttr(path, "user."+name, value.getBytes());
    }
    public static Map<String, byte[]> getAttr(String filePath) throws IOException {
        fileSystem = FileSystem.get(config);
        Path path = new Path(filePath);


        Map<String, byte[]> xattrs = fileSystem.getXAttrs(path);
       /* for (Map.Entry<String, byte[]> entry : xattrs.entrySet()) {
            String xAttrName = entry.getKey();
            byte[] xAttrValue = entry.getValue();
            String encodedValue = new String(xAttrValue, StandardCharsets.UTF_8);
            System.out.println("XAttr: " + xAttrName + " Value: " + encodedValue);
        }*/

        System.out.println(fileSystem.getFileStatus(path));
        return xattrs;
    }


}
