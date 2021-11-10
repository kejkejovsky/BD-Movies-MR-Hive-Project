import com.sun.jersey.core.util.LazyVal;

import org.apache.hadoop.io.Text;

public class testClass {
    public static void main(String[] args){
        System.out.println("Hello world");
        String text = new String("tt0000001\t1\tnm1588970\tself\t\\N\t[\"Herself\"]");
        Text category = new Text();
        Integer i = 0;
        for(String word : text.split("\t")){
            System.out.println(word);
            if(i == 3){
                category.set(word);
            }
            i++;
        }
        System.out.println(category);
        if(text.contains("self")){
            System.out.println('1');
        }
    }
}
