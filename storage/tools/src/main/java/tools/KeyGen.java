package tools;

import java.util.Random;

public class KeyGen {
	
	protected static final char [] chars = {
		'0','1','2','3','4','5','6','7','8','9',		
		'a','b','c','d','e','f','g','h','i','j',
		'k','l','m','n','o','p','q','r','s','t',
		'u','v','w','x','y','z',		
		'A','B','C','D','E','F','G','H','I','J',
		'K','L','M','N','O','P','Q','R','S','T',
		'U','V','W','X','Y','Z'
	};
	
	public static String uuid(int length,int redix){
		if (length <= 0){
			return uuid();
		}
		
		int r = redix <= 0 || redix > chars.length ? chars.length : redix;
		int l = length <= 0 ? 20 : length;
		
		char [] uuid = new char[length];
		
		Random rand = new Random();
		for (int i = 0 ;i < l ; i ++){
			uuid[i] = chars[rand.nextInt(r) % r];
		}
		
		return new String(uuid);
	}
	
	public static String uuid(int length,int start,int end){
		if (length <= 0){
			return uuid();
		}
		
		int e = end < 0 || end >= chars.length ? chars.length - 1 : end;
		int s = start < 0 || start >= e ? e: start;
		int l = length <= 0 ? 20 : length;

		char [] uuid = new char[length];
		
		Random rand = new Random();
		for (int i = 0 ;i < l ; i ++){
			uuid[i] = chars[s + rand.nextInt(e - s + 1) % (e - s + 1)];
		}
		
		return new String(uuid);		
	}
	
	public static String uuid(){		
		char [] uuid = new char[36];
		
		uuid[8] = uuid[13] = uuid[18] = uuid[23] = '-';
		uuid[14] = '4';
	      
		Random rand = new Random();
		for (int i = 0 ;i < 36 ; i ++){
			if (uuid[i] <= 0){
				int r = rand.nextInt(16) % 16;
				uuid[i] = chars[(i == 19) ? (r & 0x3) | 0x8 : r];
			}
		}
		
		return new String(uuid);
	}	

	public static String getKey(int width){
		return uuid(width,0);
	}
	
	public static String getKey(){
		return uuid(20,0);
	}
	
	public static String num(int length){
		return uuid(length,0,9);
	}
	
	public static String lowercase(int length){
		return uuid(length,10,35);
	}
	
	public static String uppercase(int length){
		return uuid(length,36,61);
	}
	
}
