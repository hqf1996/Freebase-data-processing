package util;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

public class Util {
	/**
	 * 从hadoop的分布式缓存中读取信息，存放在list<String[]>中
	 * @param dirPath
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static List<String[]> getListFromDir(String dirPath, String splitStr, int num)throws IOException, InterruptedException{
		List<String> fileList = new ArrayList<String>();
		getAllFile(dirPath, fileList);
		List<String[]> list = new ArrayList<String[]>();
		for(String filePath: fileList){
			InputStreamReader isr = new InputStreamReader(new FileInputStream(filePath),"utf-8");
			BufferedReader br = new BufferedReader(isr);
			String line;
			while((line = br.readLine()) != null){
				String[] strs = line.split(splitStr);
				if(strs.length >= num){//防止越界
					list.add(strs);
				}
			}
			br.close();
			isr.close();
			}
		return list;
	}
	
	public static void getAllFile(String dirPath, List<String> fileList){
		File file = new File(dirPath);
		if (file.isDirectory()){
			for(String path: file.list()){
				getAllFile(file.getPath() + "/" + path, fileList);
			}
		}else {
			if (!file.getName().startsWith(".") && !file.getName().startsWith("_")){
				System.out.println(file.getName());
				fileList.add(dirPath);
			}
		}
	}
	
	public static List<String[]> getListFromDir(String dirPath,int num)throws IOException, InterruptedException{
		return getListFromDir(dirPath, "'", num);
	}
	
	
	/**
	 * 从hadoop的分布式缓存中读取信息，存放在map中
	 * @param dirPath
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static Map<String, String> getMapFromDir(String dirPath, String split,int keyPlace, int valuePlace)throws IOException, InterruptedException{
		List<String> fileList = new ArrayList<String>();
		getAllFile(dirPath, fileList);
		Map<String, String> map = new HashMap<String, String>();
		for(String filePath: fileList){
			InputStreamReader isr = new InputStreamReader(new FileInputStream(filePath),"utf-8");
			BufferedReader br = new BufferedReader(isr);
			String line;
			while((line = br.readLine()) != null){
				String[] strs = line.split(split);
				if(strs.length > (keyPlace>valuePlace?keyPlace:valuePlace)){
					map.put(strs[keyPlace], strs[valuePlace]);
				}
			}
			br.close();
			isr.close();
		}
		return map;
	}
	
	public static Map<String, String> getMapFromDir(String dirPath, int keyPlace, int valuePlace)throws IOException, InterruptedException{
		return getMapFromDir(dirPath, "'", keyPlace, valuePlace);
	}
	
	public static Map<String, String> getMapFromDir(String dirPath)throws IOException, InterruptedException{
		return getMapFromDir(dirPath, 0, 1);
	}
	
	/**
	 * 获取当前的时间
	 * @return String yyyy-MM-dd的时间格式
	 */
	public static String getLocalData(){
		Date current = new Date();
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		return dateFormat.format(current);
	}
	
	public static String getSplitFirst(String s, String regex){
		String[] arr = s.split(regex);
		if(arr.length < 1){
			return "";
		}else {
			return arr[0];
		}
	}
}
