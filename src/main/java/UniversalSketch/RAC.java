package UniversalSketch;

import org.apache.commons.collections.BufferOverflowException;

import java.io.Serializable;
import java.util.Random;

public class RAC implements Serializable {
    private static final int L_a = 4;
    private static final int L_b = 11;
    private short rac;
    private final Random rand;

    public RAC(){
        this.rac = 0;
        this.rand = new Random(12345);
    }

    public RAC(int seed){
        this.rac = 0;
        this.rand = new Random(seed);
    }

    public int get_value(){
        int rawRac = this.rac & 0xFFFF;
        int rho = (rawRac >> (L_a+L_b)) & 0x1;
        int a = (rawRac >> L_b) & 0xF;
        int b_approx = (1 << L_b)+(rawRac & 0x7FF);
        return (2*rho-1)*(b_approx*(1 << a)-(1 << L_b));
    }

    public void add(int w) throws BufferOverflowException{
        int rawRac = this.rac & 0xFFFF;
        int rho = (rawRac >> (L_a+L_b)) & 0x1;
        int a = (rawRac >> L_b) & 0xF;
        int beta = (rawRac & 0x7FF);

        int beta_approx = (1 << L_b) + beta + (2*rho-1)*roundedDiv(w, (1 << a));
        int a_approx = a;

        if(beta_approx == 0){
            rho = 1-rho;
            beta_approx = (1 << L_b);
            a_approx = 1;
        }

        if( (beta_approx*(1 << a_approx)-(1 << L_b)) <  0 ){
            rho = 1- rho;
            beta_approx = roundedDiv(( (1 << (L_b+1))-beta_approx*(1 << a_approx) ), (1 << a_approx));
        }

        while( (beta_approx < (1 << L_b)) ||  (beta_approx >= (1 << (L_b+1))) ){
            //int deltaAlpha = (int) (L_b-Math.floor(Math.log(beta_approx)/Math.log(2)));
            int deltaAlpha = L_b - (31 - Integer.numberOfLeadingZeros(beta_approx));
            a_approx -= deltaAlpha;

            if(deltaAlpha > 0)
                beta_approx *= (1 << deltaAlpha);
            else
                beta_approx = roundedDiv(beta_approx, (int)Math.pow(2, -deltaAlpha));
        }

        if( a_approx >= (1 << L_a) )
            throw new BufferOverflowException("An Overflow Exception has occurred...");
        else
            store_rac(rho, a_approx, beta_approx);

    }

    private void store_rac(int rho, int a_approx, int beta_approx){
        int beta = beta_approx - (1 << L_b);
        this.rac = (short) (((rho & 0x1) << (L_a + L_b)) | ((a_approx & 0xF) << L_b) | (beta & 0x7FF));
    }

    private int roundedDiv(int w, int v){
        if(w < 0)
            return -roundedDiv(-w, v);
        if(w >= v)
            return  (w / v) +roundedDiv(w % v, v);

        return (rand.nextInt(v) < w) ? 1 : 0;
    }

    public short getRac() {
        return rac;
    }

    public void setRac(short rac) {
        this.rac = rac;
    }

    public Random getRand() {
        return rand;
    }
}
