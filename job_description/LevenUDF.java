
import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import java.lang.reflect.Method;

public class LevenUDF extends EvalFunc<Integer>
{
	public Integer exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0)
 		return null;
		try{
		Levenshtein l = new Levenshtein();
		String ads = (String)input.get(0);
		String dic = (String)input.get(1);
		return l.getLevenshteinDistance(ads, dic);
		}catch(Exception e){
		throw new IOException("Caught exception processing input row ", e);
		}
    }
}