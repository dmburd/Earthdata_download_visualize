import os
import pprint
from pathlib import Path

import earthaccess
import hydra
import xarray as xr
from omegaconf import DictConfig

pp = pprint.PrettyPrinter(indent=4)


def get_search_data_results(date_min: str, date_max: str) -> list[str]:
    temporal = (
        f"{date_min}T00:00:00Z",
        f"{date_max}T23:59:59Z"
    )

    results = earthaccess.search_data(
        short_name="GPM_2ADPR",
        temporal=temporal,
        count=-1,
    )

    return results


def find_opendap_url(granule_meta: dict) -> str | None:
    """Return the first OPeNDAP URL found in the granule UMM RelatedUrls (or None)."""
    related = granule_meta.get("umm", {}).get("RelatedUrls", []) or []
    for r in related:
        url = r.get("URL") or r.get("url") or r.get("Href") or r.get("Href") if isinstance(r, dict) else None
        if not url:
            continue
        if "opendap" in url.lower():
            return url
        # sometimes Type field holds the service type
        if "Type" in r and (("opendap" in (r.get("Type") or "").lower()) or ("OPeNDAP" in (r.get("Type") or ""))):
            return url

    # fallback: sometimes earthaccess.data_links() returns https download links only
    # but many collections include OPENDAP in 'RelatedUrls' as above
    return None


@hydra.main(config_path='.', config_name='config', version_base=None)
def main(cfg: DictConfig) -> None:
    if 'EARTHDATA_USERNAME' not in os.environ:
        os.environ['EARTHDATA_USERNAME'] = cfg.earthdata_access.username

    if 'EARTHDATA_PASSWORD' not in os.environ:
        os.environ['EARTHDATA_PASSWORD'] = cfg.earthdata_access.password

    # initialize an authenticated session
    session = earthaccess.login(strategy='environment')   # uses EARTHDATA_TOKEN
    print('Authenticated?', session.authenticated)
    print(f"{session=}")
    print(f"{session.get_session()=}")

    results = get_search_data_results(
        cfg.selection.date_min,
        cfg.selection.date_max
    )

    for g in results:
        title = g.get("meta", {}).get("title") or g.get("meta", {}).get("granule_ur") or g.get("umm", {}).get("GranuleUR")
        print("\n=== Granule:", title)
        opendap_base = find_opendap_url(g)
        if not opendap_base:
            print(" No OPeNDAP URL found in RelatedUrls for this granule; skipping.")
            continue

        subset_url_dap4 = opendap_base.replace("https://", "dap4://")

        ds_xr = xr.open_dataset(
            subset_url_dap4,
            engine='pydap',
            session=session.get_session(),
            decode_timedelta=True,
        )
        
        to_save = xr.Dataset()
        var_name_underscores = [
            var_name_slashes[1:].replace('/', '_')
            for var_name_slashes in cfg.observable_vars
        ]
        to_save = ds_xr[var_name_underscores]

        transformed_granume_name = title.split(':')[1]
        outfile = Path(cfg.partial_mirror.rootdir) / f"{transformed_granume_name}.nc"
        to_save.to_netcdf(outfile, format="NETCDF4")


if __name__ == "__main__":
    main()


## cd create_partial_mirror
## python main.py
