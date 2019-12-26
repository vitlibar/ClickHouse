#include <Interpreters/IExternalLoaderConfigRepository.h>
#include <Interpreters/ExternalLoader.h>


namespace DB
{
IExternalLoaderConfigRepository::~IExternalLoaderConfigRepository()
{
    if (auto * p = external_loader.load())
        p->removeConfigRepository(this);
}
}
