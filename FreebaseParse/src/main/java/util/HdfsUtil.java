package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class HdfsUtil {

	public static void createDir(Configuration conf, String uri, Logger LOG){
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI(uri), conf);
			Path dfs = new Path(uri);
			if(!fs.exists(dfs)){
				if(fs.mkdirs(dfs)){
					LOG.info("mkdir dir success !");
				}else {
					LOG.error("mkdir dir fail !");
				}
			}else {
				LOG.info("dir is already exists !");
			}
		} catch (Exception e) {
			// TODO Auto-generated catch block
			LOG.error("mkdir dir fail !");
		}finally {
			if(fs != null){
				try {
					fs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					LOG.error("hdfs connect close fail !");
				}
			}
		}

	}
	
	public static void mvFile(Configuration conf, String source, String target, Logger LOG){
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI(source), conf);
			Path sourcePath = new Path(source);
			Path targetPath = new Path(target);
			if(fs.rename(sourcePath, targetPath)){
				LOG.info("move is success :" + source + "\tto\t" + target);
			}else {
				LOG.error("move is fail");
			}
		} catch (Exception e) {
			// TODO: handle exception
			LOG.error("move is fail");
		}finally {
			if(fs != null){
				try {
					fs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					LOG.error("hdfs connect close fail !");
				}
			}
		}
	}
	
	public static void removeFile(Configuration conf, String file, Logger LOG){
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI(file), conf);
			Path filePath = new Path(file);
			if(fs.delete(filePath,false)){
				LOG.info("remove is success");
			}else {
				LOG.error("remove is fail");
			}
		} catch (Exception e) {
			// TODO: handle exception
			LOG.error("remove is fail");
		}finally {
			if(fs != null){
				try {
					fs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					LOG.error("hdfs connect close fail !");
				}
			}
		}
	}
	
	public static void removeFile(Configuration conf, List<String> files, Logger LOG){
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new URI(files.get(0)), conf);
			for(String file:files){
				Path filePath = new Path(file);
				if(fs.delete(filePath,false)){
					LOG.info("remove " + filePath + " is success");
				}else {
					LOG.error("remove " + filePath + " is fail");
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			LOG.error("remove is fail");
		}finally {
			if(fs != null){
				try {
					fs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					LOG.error("hdfs connect close fail !");
				}
			}
		}
	}
	
	
	public static List<String> listAll(Configuration conf, String dir, Logger LOG){
		FileSystem fs = null;
		List<String> paths = new ArrayList<String>();
		try {
			fs = FileSystem.get(new URI(dir), conf);
			Path dirPath = new Path(dir);
			if(fs.isDirectory(dirPath)){
				FileStatus[] fileStatus = fs.listStatus(dirPath);
				for(int i = 0;i<fileStatus.length;i++){
					paths.add(fileStatus[i].getPath().toString());
				}
			}else {
				LOG.info("this is not a  directory");
			}
		} catch (Exception e) {
			// TODO: handle exception
			LOG.error("remove is fail");
		}finally {
			if(fs != null){
				try {
					fs.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					LOG.error("hdfs connect close fail !");
				}
			}
		}
		return paths;
	}
}
