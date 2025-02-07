# Gallica to Bibliography

## Citation

If (somehow) you need to include a citation of this script, you may use these metadata:
> * **Author** : Morgane PICA
> * **Title** : "Gallica to bibliography"
> * **Initial upload** : 2025/02/07
> * **Language** : Python 3.11.5
> * **Dependencies** :
>   * NumPy - 1.26.4
>   * Pandas - 2.2.3
>   * Python-docx - 1.1.2
>   * SPARQLWrapper - 2.0.0
>   * tqdm - 4.66.5


## General description

This set of functions takes a list of links of BnF catalogue or Gallica URIs/URLs, gets metadata from the DataBnF SPARQL endpoint, and then transform these metadata into a DOCX bibliography.

Please note, firstly that it was made as a one-shot script and only takes into account the *Oxford style* or *Author-date* bibliographical rules :
> SMALL-CAPS-NAME, Firstname \[1st edition] (current edition). *Title of book*. Number of tomes/volumes. Publishing place : publisher. On line : Gallica-URL.

However, the script is divided in two functions, and you can use the result of the first (a XLSX table with all harvested metadata) as an already big help in your bibliographical creation.

Secondly, this will not necessarily produce a final bibliography and will need checking, mostly:
* DataBnF does not record the order between author names.
* No first editions were recorded by DataBnF for my test corpus, so I only left room for it in the output.

I may possibly look into that later (at some point and if I have time).

## How to use *Gallica to Bibliography*

The `Gallica2biblio` script is only meant to facilitate the use of these functions, it is not necessary in order to actually run the functions from `utils.py`.

The main functions are:
* `parse_list(path_to_file)`, which takes your list of URLs/URIs as a TXT file with one URL/URI per line. Anything which does not contain `bnf.fr` will simply be ignored, so feel free to include other information, as long as every URL/URI is the only thing in its line.
* `layout(path_to_file)`, which takes the path to the XLSX file produced by the previous function, which should be named `iiif_metadata.xlsx`.
