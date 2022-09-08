import sortinghat.exceptions
from sortinghat import api
from sortinghat.db.database import Database
from sortinghat.matcher import create_identity_matcher
import logging


def merge_wrapper(ids_to_merge):
    if not ids_to_merge or len(ids_to_merge) == 1:
        return
    for i in range(len(ids_to_merge) - 1):
        try:
            api.merge_unique_identities(db, str(ids_to_merge[i]), str(ids_to_merge[i+1]))
        except sortinghat.exceptions.NotFoundError:
            pass

#
db = Database('root', '', 'test_sh')
# sgoggins = api.search_unique_identities(db=db, term='hqrshguptq')

# print(sgoggins) # should return [44bdeb72febce7cd43244acdee5dab8dd6a702d8, e12ec3126952dc0a8edebd5f122c388ff53bf9f2]
# for profile in list(api.search_profiles(db=db)):
    # if 'sgoggins' in str(profile):
    #     print(type(profile))
    # print(profile)
        # pass



def combine_identities():
    matcher = create_identity_matcher()
    for profile in api.search_profiles(db):

        try:
            for unique_identity in api.match_identities(db, profile.uuid, matcher=matcher): # not found in registry
                for identity in unique_identity.identities: # TODO optimize for set
                    if identity.username:
                        merge_wrapper(api.search_unique_identities(db=db, term=identity.username)) #Search for all identities that have this username listed
        except sortinghat.exceptions.NotFoundError:
            pass


def combine_identities(term, sources):
    """
    Searches unique identities stored in Sorting Hat for the term parameter from each of the sources specified
    then combines all of them together.
     - Source values:
        - github (name + email)
        - githubql (github login: username)
        - github2
    Sources are sorted by alphabetical order so they will take on the original uuid of the
    lexicographically smallest source name.


    :param str term: Term to search by
    :param list[str] sources: List of sources to search in
    :return: List of Unique Identity objects
    :rtype: List[UniqueIdentity]

    """
    unique_identities = []
    sources.sort()
    for source in sources:
        try:
            unique_identities += list(api.search_unique_identities(db=db, term=term, source=source))
        except sortinghat.exceptions.NotFoundError:
            pass
    merge_wrapper(unique_identities) #TODO get rid of duplicates
    try:
        return api.search_unique_identities(db=db, term=term, source=sources[0])  # TODO can this ever return >1
    except sortinghat.exceptions.NotFoundError:
        return -1



# print(combine_identities('sgoggins', ['githubql', 'github']))
#api.search_unique_identities(db=db, term='sgoggins', source='github')












# print(api.unique_identities(db))

# matcher = create_identity_matcher()
#
# print(api.match_identities(db=db, uuid='b7a2e25d434c7ca43687dbff56c1e482c85eef05', matcher=matcher))
#


# from_uuid = 'a9b403e150dd4af8953a52a4bb841051e4b705d9'
# to_uuid = '3de180633322e853861f9ee5f50a87e007b51058'
#
# api.merge_unique_identities(db=db, from_uuid=from_uuid, to_uuid=to_uuid)