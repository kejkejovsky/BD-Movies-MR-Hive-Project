import javax.xml.soap.Text;

public class testClass {
    public static void main(String[] args){
        System.out.println("Hello world");
        String text = new String("tt0000001\t1\tnm1588970\tself\t\\N\t[\"Herself\"]");
        for(String word : text.split("\t")){
            System.out.println(word);
        }
    }
}
