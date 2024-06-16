import java.io.*;

public class RemoveTime {
    public static void main(String[] args) {
        String inputFile = "/Users/draco/IdeaProjects/sqlancer/logs/mysql/database0-cur.log"; // 输入文件路径
        String outputFile = "output.sql"; // 输出文件路径

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile));
             BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {

            String line;
            while ((line = reader.readLine()) != null) {
                // 找到 "-- "的位置并截取前面的SQL语句部分
                int idx = line.indexOf("; --");
                if (idx != -1) {
                    line = line.substring(0, idx + 1); // 包含分号
                }
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            System.out.println("发生IO异常: " + e.getMessage());
        }
    }
}
