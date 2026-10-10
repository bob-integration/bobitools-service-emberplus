# Ember+ — service Bobi.Tools

Provider **Ember+** générique pour [Bobi.Tools](https://github.com/bob-integration/bobitools) :
il rassemble ce que les outils installés choisissent d'exposer et le publie en Ember+ (S101 sur
TCP), lisible et pilotable par un contrôleur broadcast (VSM ou autre).

Un service n'apparaît pas au lanceur : il se règle dans **Réglages → Protocoles → Ember+**.

## Ce que fait le service

- **Écoute en TCP** (port 9000 par défaut) et sert un arbre Ember+ encodé en Glow : nœuds,
  paramètres (texte, entier, réel, booléen, énumération) et matrices (oneToN, oneToOne, NToN).
- **Agrège les contributions des outils.** Chaque outil contributeur reçoit sa propre racine
  numérotée ; ce numéro est mémorisé, donc stable d'un redémarrage à l'autre, et peut être
  imposé à la main depuis les réglages.
- **Route les écritures** (SetValue, points de croisement) vers l'outil propriétaire. Chaque
  écriture est journalisée au nom de « Service Ember+ » et passe par les mêmes règles de
  périmètre que l'interface (Réglages → Outils → Périmètres).
- **Ne diffuse que ce qui change** : l'arbre est reconstruit à intervalle régulier (5 s par
  défaut) et seuls les éléments modifiés partent aux abonnés. Un outil peut aussi signaler un
  changement pour qu'il parte aussitôt.
- **Garde le dernier état connu** d'un contributeur lent ou arrêté : ses chemins ne
  disparaissent pas de l'arbre du contrôleur.
- **Écoute secondaire facultative** : un second port qui ne sert que certaines racines, pour un
  contrôleur qui veut s'abonner à une branche sans subir les renvois du reste de l'arbre.

## Contribuer à l'arbre (outils)

Un outil devient contributeur en déclarant `"ember": true` dans son `plugin.json` et en
répondant à deux routes, appelées par le service qu'il soit in-process ou en conteneur :

- `GET ember/tree` → `{ "label": …, "nodes": [ {id, label, desc?, nodes?, params?} ] }`, où
  chaque paramètre porte `id`, `label`, `type`, `value`, `writable?`, `enum?` et un `ref`
  opaque ;
- `POST ember/set` ← `{ "ref": …, "value": … }` : le service rejoue le `ref` tel quel.

Un nœud qui porte `matrix` est publié comme matrice ; ses connexions arrivent sur
`POST ember/connect`. Un outil en conteneur peut signaler du neuf par `POST /api/ember/notify`
(session, ou en-tête `X-BT-Ember-Token` avec le jeton `emberplus_notify_token`).

Contributeurs publics :
[Démo matrice Ember+](https://github.com/bob-integration/bobitools-plugin-ember_matrix_demo)
(matrice 4×4 en mémoire, pour essayer sans matériel),
[Pilotage de switch](https://github.com/bob-integration/bobitools-plugin-switch_ports) et
[Caméras](https://github.com/bob-integration/bobitools-plugin-ptz).
Pour lire l'arbre publié tel qu'un contrôleur le voit :
[Ember+ Reader](https://github.com/bob-integration/bobitools-plugin-ember_reader).

Le service sait aussi publier un **profil IPG** : une numérotation fixe (grilles de flux,
affectation par slot, SDP par voie) qui présente des passerelles IP de familles différentes
sous les mêmes chemins. Ce mode n'est actif que si une couche IPG est installée ; sans elle,
la racine correspondante n'est pas émise.

## Prérequis

- Aucun : le service tourne dans Bobi.Tools, sans dépendance Python supplémentaire.
- Au moins un outil contributeur, sans quoi l'arbre n'a rien à montrer.
- Un contrôleur broadcast compatible Ember+ (S101/TCP).

## Installation

Dans Bobi.Tools : **Réglages → Outils → Catalogue**, bouton « Installer ». Ou, sur une machine
neuve, en une ligne :

```bash
bash <(curl -fsSL https://raw.githubusercontent.com/bob-integration/bobitools/main/get.sh) --outils emberplus
```

Un service se charge au démarrage : redémarrer Bobi.Tools après l'installation. Il est
désactivé par défaut : l'activer dans **Réglages → Protocoles → Ember+**.

## Sécurité

- **Ember+ ne prévoit aucune authentification.** Quiconque joint le port peut lire l'arbre :
  filtrer l'accès au niveau réseau (VLAN, pare-feu).
- **Restreindre les écritures** avec la liste d'adresses autorisées (`emberplus_write_allow`) :
  seul le contrôleur déclaré écrit, tout le monde peut lire. Liste vide = aucune restriction,
  ce que le service signale au démarrage.
- Pour soumettre un contrôleur aux règles de périmètre, le déclarer dans
  **Réglages → Protocoles → Contrôleurs**.

## In English

Generic **Ember+ provider** (S101 over TCP, default port 9000) for Bobi.Tools. It aggregates
the subtrees of every tool that declares `"ember": true` and serves `ember/tree` /
`ember/set` (plus `ember/connect` for matrices), gives each tool a stable root, routes writes
back to the owning tool with audit and access rules, and only pushes what changed. Configure it
under Settings → Protocols → Ember+. Ember+ has no authentication: filter the port at network
level and restrict writes to your controller's address.

## Licence

GPL-3.0-or-later — © 2026 BOBI SAS. Voir [LICENSE](LICENSE).
