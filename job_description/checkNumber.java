public class checkNumber {

public boolean parseWithFallback(String s) {
    if (s == null) {
    throw new IllegalArgumentException("Strings must not be null");
    }

    try {
     Integer.parseInt(s);
    return true;
    } catch (NumberFormatException e) {
    return false;
     } 
    }
}
