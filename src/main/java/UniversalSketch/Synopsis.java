package UniversalSketch;

import messages.Estimation;
import messages.Request;

abstract public class Synopsis {

	protected int SynopsisID;
	
	public Synopsis(int ID) {
		SynopsisID=ID;
	}

    public abstract void add(Object k);
	public abstract Object estimate(Object k);
	public abstract Estimation estimate(Request rq);
	public abstract Synopsis merge(Synopsis sk);


	public int getSynopsisID() {
		return SynopsisID;
	}
	public void setSynopsisID(int SynopsisID) {
		this.SynopsisID = SynopsisID;
	}
}
