#ifndef MYRAND_MYRAND_H__
#define MYRAND_MYRAND_H__

#include <cstdlib>

namespace myrand
{
class MyRand
{
public:
	explicit MyRand(int min, int max);
	~MyRand() { }

	int generate()
	{ return (fMin + (int)((double)(fMax - fMin + 1) * (rand_r(&fSeed) / (RAND_MAX + 1.0)))); }

	int operator()() { return generate(); }

protected:

private:
	//defaults okay (I guess)
	//MyRand(const MyRand& rhs);
	//MyRand& operator=(const MyRand& rhs);

	unsigned int fSeed;
	int fMin;
	int fMax;
};
}

#endif
// vim:ts=4 sw=4:

