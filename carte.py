import matplotlib.pyplot as plt
#pip3 install cartopy
import cartopy.crs as ccrs
import cartopy.feature as cfeature

def plot_points(df):
    # Convertir le DataFrame Spark en DataFrame pandas pour le traçage
    pandas_df = df.toPandas()

    # Créer une figure et un axe
    fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()})

    # Ajouter les caractéristiques géographiques
    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.OCEAN)
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    ax.add_feature(cfeature.LAKES, alpha=0.5)
    ax.add_feature(cfeature.RIVERS)

    # Tracer les points
    ax.scatter(pandas_df['avg(longitude)'], pandas_df['avg(latitude)'])

    # Afficher la figure
    plt.show()