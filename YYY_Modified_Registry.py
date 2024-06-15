   if raw_overhanging_pv_installations_enriched_with_closest_rooftop_data is None:
            return None
        # Centroid coordinates of intersected pv polygons
        address_points = list(
            zip(
                raw_overhanging_pv_installations_enriched_with_closest_rooftop_data[
                    "centroid_intersect"
                ].x,
                raw_overhanging_pv_installations_enriched_with_closest_rooftop_data[
                    "centroid_intersect"
                ].y,
            )
        )

        if gdA.empty or gdB.empty:
            return None
        # List specifying the centroid coordinates of the overhanging PV polygons
        nA = np.array(list(zip(gdA.geometry.x, gdA.geometry.y)))





        if raw_PV_installations_off_rooftop.empty:
            print(f"Warning: 'raw_PV_installations_off_rooftop' is empty for region {self.county}. Skipping overhanging PV processing.")
            return [raw_PV_installations_on_rooftop, None]

        return [raw_PV_installations_on_rooftop, raw_PV_installations_off_rooftop]




        if raw_overhanging_pv_installations_enriched_with_closest_rooftop_data is None:
            return None
        raw_overhanging_pv_installations_enriched_with_closest_rooftop_data[
            "area_inter"
        ] = raw_overhanging_pv_installations_enriched_with_closest_rooftop_data[
            "area_diff"
        ]