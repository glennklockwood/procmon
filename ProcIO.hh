class ProcIO {
public:

	virtual bool write_procstat(procstat* start_ptr, int count);
	virtual bool write_procdata(procdata* start_ptr, int count);

	
protected:
	std::string cluster;
	std::string hostname;
	std::string identifier;
	std::string subidentifier;

