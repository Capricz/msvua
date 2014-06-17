import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;


public class ChangeNameTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		String folderName = "C:\\workspace\\git\\msvua\\src\\main\\java\\com\\hp";
		changeFileName(new File(folderName));
	}

	private static void changeFileName(File file) {
			if(file!=null && file.isDirectory()){
				String[] list = file.list();
				for (int i = 0; i < list.length; i++) {
					String fName = list[i];
//					String childPath = file.getAbsolutePath()+"\\"+fName;
					changeFileName(new File(file.getAbsolutePath(),fName));
				}
			} else if(file != null){
				FileReader fr = null;
				BufferedReader br = null;
				FileWriter fw = null;
				BufferedWriter bw = null;
				try {
					fr = new FileReader(file);
					br = new BufferedReader(fr);
					fw = new FileWriter(file);
					bw = new BufferedWriter(fw);
					String line = "";
					String regex = "com\\.hp\\.datamigration\\.trans";
	//				Pattern p = Pattern.compile();
					StringBuffer sb = new StringBuffer();
//					while((line=br.readLine())!=null){
					while(br.readLine() != null){
						System.out.println(br.readLine());
//						sb.append(line.replaceAll(regex, "com.hp.msvua"));
					}
					if(sb.toString() != null){
						bw.write(sb.toString());
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally{
					try {
						fr.close();
						fw.close();
						br.close();
						bw.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
					
				}
			}
	}

}
